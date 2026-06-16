"""
PostgreSQL 14+ → Aurora DSQL CDC Replication — Lambda Handler (Polling Mode)
=============================================================================
Polling-based CDC consumer for AWS Lambda deployment.
Use as a fallback/catch-up mode alongside the 24/7 ECS streaming service.

Decoding plugins supported (built-in, NO extensions required):
  - test_decoding (default) — built into every PostgreSQL installation
  - pgoutput — native PG 10+ logical replication protocol

Architecture:
  PostgreSQL (source) → Logical Replication Slot → Lambda (poll) → Aurora DSQL (target)
                                                               └→ S3 (archive/audit)

Environment Variables:
  SOURCE_DSN          - PostgreSQL source connection string
  TARGET_DSN          - Aurora DSQL connection string
  SLOT_NAME           - Logical replication slot name (default: dsql_cdc_slot)
  PUBLICATION_NAME    - Publication name (default: dsql_cdc_pub)
  DECODING_PLUGIN     - test_decoding | pgoutput (default: test_decoding)
  S3_BUCKET           - S3 bucket for change archival
  S3_PREFIX           - S3 key prefix (default: cdc-events/)
  CONFLICT_MODE       - One of: upsert, skip, fail, last_write_wins (default: upsert)
  BATCH_SIZE          - Number of changes per batch (default: 1000)
  MAX_POLL_SECONDS    - Max seconds to poll before flushing (default: 30)
  CHECKPOINT_TABLE    - Table for LSN checkpoints (default: _cdc_checkpoint)
  TABLES              - Comma-separated list of tables to replicate (default: all)
  PARALLEL_WORKERS    - Number of parallel DSQL writers (default: 4)
"""


import json
import os
import re
import time
import logging
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
import psycopg2.extras
import boto3

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

logger = logging.getLogger("pg_dsql_cdc")
logger.setLevel(logging.INFO)


class ConflictMode(str, Enum):
    UPSERT = "upsert"
    SKIP = "skip"
    FAIL = "fail"
    LAST_WRITE_WINS = "last_write_wins"


class DecodingPlugin(str, Enum):
    TEST_DECODING = "test_decoding"
    PGOUTPUT = "pgoutput"


@dataclass
class CDCConfig:
    source_dsn: str
    target_dsn: str
    slot_name: str = "dsql_cdc_slot"
    publication_name: str = "dsql_cdc_pub"
    decoding_plugin: DecodingPlugin = DecodingPlugin.TEST_DECODING
    s3_bucket: str = ""
    s3_prefix: str = "cdc-events/"
    conflict_mode: ConflictMode = ConflictMode.UPSERT
    batch_size: int = 1000
    max_poll_seconds: int = 30
    checkpoint_table: str = "_cdc_checkpoint"
    dlq_bucket: str = ""
    tables: list = field(default_factory=list)
    parallel_workers: int = 4

    @classmethod
    def from_env(cls) -> "CDCConfig":
        tables_str = os.environ.get("TABLES", "")
        tables = [t.strip() for t in tables_str.split(",") if t.strip()] if tables_str else []
        return cls(
            source_dsn=os.environ["SOURCE_DSN"],
            target_dsn=os.environ["TARGET_DSN"],
            slot_name=os.environ.get("SLOT_NAME", "dsql_cdc_slot"),
            publication_name=os.environ.get("PUBLICATION_NAME", "dsql_cdc_pub"),
            decoding_plugin=DecodingPlugin(os.environ.get("DECODING_PLUGIN", "test_decoding")),
            s3_bucket=os.environ.get("S3_BUCKET", ""),
            s3_prefix=os.environ.get("S3_PREFIX", "cdc-events/"),
            conflict_mode=ConflictMode(os.environ.get("CONFLICT_MODE", "upsert")),
            batch_size=int(os.environ.get("BATCH_SIZE", "1000")),
            max_poll_seconds=int(os.environ.get("MAX_POLL_SECONDS", "30")),
            checkpoint_table=os.environ.get("CHECKPOINT_TABLE", "_cdc_checkpoint"),
            dlq_bucket=os.environ.get("DLQ_BUCKET", ""),
            tables=tables,
            parallel_workers=int(os.environ.get("PARALLEL_WORKERS", "4")),
        )


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

@dataclass
class ChangeEvent:
    """Represents a single DML change from the WAL."""
    lsn: str
    timestamp: str
    schema: str
    table: str
    operation: str  # INSERT, UPDATE, DELETE
    columns: list  # column definitions
    old_values: Optional[dict]  # previous row (UPDATE/DELETE)
    new_values: Optional[dict]  # new row (INSERT/UPDATE)
    xid: int = 0

    @property
    def fqtn(self) -> str:
        return f'"{self.schema}"."{self.table}"'

    @property
    def primary_keys(self) -> List[str]:
        return [c["name"] for c in self.columns if c.get("pk", False)]


# ---------------------------------------------------------------------------
# test_decoding Parser
# ---------------------------------------------------------------------------

class TestDecodingParser:
    """
    Parses test_decoding output format.
    
    Output format:
      table public.orders: INSERT: id[integer]:1 name[text]:'hello' price[numeric]:9.99
      table public.orders: UPDATE: old-key: id[integer]:1 new-tuple: id[integer]:1 name[text]:'world'
      table public.orders: DELETE: id[integer]:1
    """

    _TABLE_PATTERN = re.compile(
        r"^table\s+(?P<schema>\w+)\.(?P<table>\w+):\s+(?P<op>INSERT|UPDATE|DELETE):\s+(?P<data>.*)$"
    )
    _COLUMN_PATTERN = re.compile(
        r"(?P<name>\w+)\[(?P<type>[^\]]+)\]:(?P<value>(?:'(?:[^'\\]|\\.)*'|[^\s]+))"
    )

    def __init__(self, config: CDCConfig):
        self.config = config
        self._table_pk_cache: Dict[str, List[str]] = {}

    def parse(self, data: str, lsn: str, xid: int = 0) -> List[ChangeEvent]:
        """Parse a test_decoding message into ChangeEvent objects."""
        events = []
        timestamp = datetime.now(timezone.utc).isoformat()

        for line in data.strip().split("\n"):
            line = line.strip()
            if line.startswith("BEGIN") or line.startswith("COMMIT"):
                continue

            match = self._TABLE_PATTERN.match(line)
            if not match:
                continue

            schema = match.group("schema")
            table = match.group("table")
            operation = match.group("op")
            col_data = match.group("data")

            # Table filter
            if self.config.tables:
                fqtn = f"{schema}.{table}"
                if fqtn not in self.config.tables and table not in self.config.tables:
                    continue

            if operation == "INSERT":
                new_values, columns = self._parse_columns(col_data)
                old_values = None
            elif operation == "UPDATE":
                old_values, new_values, columns = self._parse_update(col_data)
            elif operation == "DELETE":
                old_values, columns = self._parse_columns(col_data)
                new_values = None
            else:
                continue

            # Mark PK columns
            pk_cols = self._get_pk_columns(schema, table)
            for col in columns:
                col["pk"] = col["name"] in pk_cols

            events.append(ChangeEvent(
                lsn=lsn, timestamp=timestamp, schema=schema,
                table=table, operation=operation, columns=columns,
                old_values=old_values, new_values=new_values, xid=xid,
            ))

        return events

    def _parse_columns(self, data: str) -> Tuple[dict, list]:
        values = {}
        columns = []
        for match in self._COLUMN_PATTERN.finditer(data):
            name = match.group("name")
            col_type = match.group("type")
            raw_value = match.group("value")
            value = self._convert_value(raw_value, col_type)
            values[name] = value
            columns.append({"name": name, "type": col_type, "pk": False})
        return values, columns

    def _parse_update(self, data: str) -> Tuple[Optional[dict], Optional[dict], list]:
        old_values = None
        new_values = None
        columns = []

        if "old-key:" in data and "new-tuple:" in data:
            parts = data.split("new-tuple:")
            old_part = parts[0].replace("old-key:", "").strip()
            new_part = parts[1].strip()
            old_values, _ = self._parse_columns(old_part)
            new_values, columns = self._parse_columns(new_part)
        elif "old-key:" in data:
            old_part = data.replace("old-key:", "").strip()
            old_values, columns = self._parse_columns(old_part)
        else:
            new_values, columns = self._parse_columns(data)

        return old_values, new_values, columns

    def _convert_value(self, raw: str, col_type: str) -> Any:
        if raw == "null":
            return None
        if raw.startswith("'") and raw.endswith("'"):
            return raw[1:-1].replace("\\'", "'").replace("\\\\", "\\")
        if col_type in ("integer", "bigint", "smallint", "int4", "int8", "int2"):
            return int(raw)
        if col_type in ("numeric", "decimal", "float4", "float8", "double precision", "real"):
            return float(raw)
        if col_type == "boolean":
            return raw.lower() in ("t", "true", "1")
        return raw

    def _get_pk_columns(self, schema: str, table: str) -> List[str]:
        key = f"{schema}.{table}"
        if key not in self._table_pk_cache:
            self._table_pk_cache[key] = self._fetch_pk_columns(schema, table)
        return self._table_pk_cache[key]

    def _fetch_pk_columns(self, schema: str, table: str) -> List[str]:
        try:
            with psycopg2.connect(self.config.source_dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT a.attname
                        FROM pg_index i
                        JOIN pg_attribute a ON a.attrelid = i.indrelid
                            AND a.attnum = ANY(i.indkey)
                        WHERE i.indrelid = %s::regclass AND i.indisprimary
                        ORDER BY array_position(i.indkey, a.attnum)
                    """, (f"{schema}.{table}",))
                    return [row[0] for row in cur.fetchall()]
        except Exception as e:
            logger.warning(f"Failed to fetch PK for {schema}.{table}: {e}")
            return []


# ---------------------------------------------------------------------------
# WAL Consumer (Polling Mode for Lambda)
# ---------------------------------------------------------------------------

class WALConsumer:
    """Consumes changes from a PostgreSQL logical replication slot using polling."""

    def __init__(self, config: CDCConfig):
        self.config = config
        self._parser = TestDecodingParser(config)

    def ensure_slot_exists(self):
        """Create replication slot if it doesn't exist."""
        with psycopg2.connect(self.config.source_dsn) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                    (self.config.slot_name,),
                )
                if not cur.fetchone():
                    cur.execute(
                        "SELECT pg_create_logical_replication_slot(%s, %s)",
                        (self.config.slot_name, self.config.decoding_plugin.value),
                    )
                    logger.info(f"Created replication slot: {self.config.slot_name} "
                                f"(plugin: {self.config.decoding_plugin.value})")
                else:
                    logger.info(f"Replication slot exists: {self.config.slot_name}")

    def consume_batch(self, last_lsn: Optional[str] = None) -> List[ChangeEvent]:
        """
        Consume a batch of changes from the replication slot.
        Uses peek mode to avoid advancing the slot until confirmed.
        """
        changes = []

        with psycopg2.connect(self.config.source_dsn) as conn:
            with conn.cursor() as cur:
                # Build options for test_decoding
                if self.config.decoding_plugin == DecodingPlugin.TEST_DECODING:
                    cur.execute(
                        """
                        SELECT lsn, xid, data
                        FROM pg_logical_slot_peek_changes(
                            %s, %s, %s,
                            'include-xids', 'true',
                            'include-timestamp', 'true',
                            'skip-empty-xacts', 'true'
                        )
                        """,
                        (self.config.slot_name, last_lsn, self.config.batch_size),
                    )
                else:
                    # pgoutput via peek
                    cur.execute(
                        """
                        SELECT lsn, xid, data
                        FROM pg_logical_slot_peek_changes(
                            %s, %s, %s,
                            'proto_version', '1',
                            'publication_names', %s
                        )
                        """,
                        (self.config.slot_name, last_lsn, self.config.batch_size,
                         self.config.publication_name),
                    )

                rows = cur.fetchall()
                for lsn, xid, data in rows:
                    try:
                        events = self._parser.parse(data, str(lsn), xid)
                        changes.extend(events)
                    except Exception as e:
                        logger.error(f"Failed to parse WAL message at LSN {lsn}: {e}")

        return changes

    def advance_slot(self, lsn: str):
        """Advance the replication slot to the given LSN (confirm consumption)."""
        with psycopg2.connect(self.config.source_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_replication_slot_advance(%s, %s)",
                    (self.config.slot_name, lsn),
                )
            conn.commit()
        logger.info(f"Advanced slot {self.config.slot_name} to LSN {lsn}")


# ---------------------------------------------------------------------------
# DSQL Writer
# ---------------------------------------------------------------------------

class DSQLWriter:
    """Applies change events to Aurora DSQL with configurable conflict resolution."""

    def __init__(self, config: CDCConfig):
        self.config = config

    def _get_connection(self):
        conn = psycopg2.connect(self.config.target_dsn)
        conn.autocommit = False
        return conn

    def apply_batch(self, events: List[ChangeEvent]) -> Tuple[int, List[ChangeEvent]]:
        if not events:
            return 0, []

        table_groups: Dict[str, List[ChangeEvent]] = {}
        for event in events:
            table_groups.setdefault(event.fqtn, []).append(event)

        success_count = 0
        failed_events = []

        with ThreadPoolExecutor(max_workers=self.config.parallel_workers) as executor:
            futures = {}
            for table, table_events in table_groups.items():
                for i in range(0, len(table_events), 500):
                    sub_batch = table_events[i:i + 500]
                    future = executor.submit(self._apply_sub_batch, sub_batch)
                    futures[future] = sub_batch

            for future in as_completed(futures):
                sub_batch = futures[future]
                try:
                    count = future.result()
                    success_count += count
                except Exception as e:
                    logger.error(f"Sub-batch failed: {e}")
                    failed_events.extend(sub_batch)

        return success_count, failed_events

    def _apply_sub_batch(self, events: List[ChangeEvent]) -> int:
        conn = self._get_connection()
        applied = 0
        try:
            with conn.cursor() as cur:
                for event in events:
                    try:
                        self._apply_event(cur, event)
                        applied += 1
                    except psycopg2.errors.UniqueViolation:
                        conn.rollback()
                        if self.config.conflict_mode == ConflictMode.FAIL:
                            raise
                        applied += 1
                    except psycopg2.errors.SerializationFailure:
                        conn.rollback()
                        self._retry_event(conn, cur, event)
                        applied += 1
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return applied

    def _apply_event(self, cur, event: ChangeEvent):
        if event.operation == "INSERT":
            self._apply_insert(cur, event)
        elif event.operation == "UPDATE":
            self._apply_update(cur, event)
        elif event.operation == "DELETE":
            self._apply_delete(cur, event)

    def _apply_insert(self, cur, event: ChangeEvent):
        if not event.new_values:
            return
        columns = list(event.new_values.keys())
        values = list(event.new_values.values())
        col_str = ", ".join(f'"{c}"' for c in columns)
        ph_str = ", ".join(["%s"] * len(columns))

        if self.config.conflict_mode == ConflictMode.UPSERT:
            pk_cols = event.primary_keys
            if pk_cols:
                update_cols = [c for c in columns if c not in pk_cols]
                update_str = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
                conflict_cols = ", ".join(f'"{c}"' for c in pk_cols)
                sql = (f"INSERT INTO {event.fqtn} ({col_str}) VALUES ({ph_str}) "
                       f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_str}")
            else:
                sql = f"INSERT INTO {event.fqtn} ({col_str}) VALUES ({ph_str})"
        elif self.config.conflict_mode == ConflictMode.SKIP:
            pk_cols = event.primary_keys
            if pk_cols:
                conflict_cols = ", ".join(f'"{c}"' for c in pk_cols)
                sql = (f"INSERT INTO {event.fqtn} ({col_str}) VALUES ({ph_str}) "
                       f"ON CONFLICT ({conflict_cols}) DO NOTHING")
            else:
                sql = f"INSERT INTO {event.fqtn} ({col_str}) VALUES ({ph_str})"
        elif self.config.conflict_mode == ConflictMode.LAST_WRITE_WINS:
            pk_cols = event.primary_keys
            if pk_cols and event.new_values:
                where = " AND ".join(f'"{c}" = %s' for c in pk_cols)
                pk_vals = [event.new_values[c] for c in pk_cols]
                cur.execute(f"DELETE FROM {event.fqtn} WHERE {where}", pk_vals)
            sql = f"INSERT INTO {event.fqtn} ({col_str}) VALUES ({ph_str})"
        else:
            sql = f"INSERT INTO {event.fqtn} ({col_str}) VALUES ({ph_str})"

        cur.execute(sql, values)

    def _apply_update(self, cur, event: ChangeEvent):
        if not event.new_values:
            return
        pk_cols = event.primary_keys
        if not pk_cols:
            logger.warning(f"UPDATE without PK on {event.fqtn}, skipping")
            return
        set_cols = [c for c in event.new_values.keys() if c not in pk_cols]
        if not set_cols:
            return
        set_str = ", ".join(f'"{c}" = %s' for c in set_cols)
        set_vals = [event.new_values[c] for c in set_cols]
        where_source = event.old_values if event.old_values else event.new_values
        where_str = " AND ".join(f'"{c}" = %s' for c in pk_cols)
        where_vals = [where_source.get(c, event.new_values.get(c)) for c in pk_cols]
        sql = f"UPDATE {event.fqtn} SET {set_str} WHERE {where_str}"
        cur.execute(sql, set_vals + where_vals)

    def _apply_delete(self, cur, event: ChangeEvent):
        pk_cols = event.primary_keys
        source = event.old_values or event.new_values
        if not pk_cols or not source:
            return
        where_str = " AND ".join(f'"{c}" = %s' for c in pk_cols)
        where_vals = [source[c] for c in pk_cols]
        cur.execute(f"DELETE FROM {event.fqtn} WHERE {where_str}", where_vals)

    def _retry_event(self, conn, cur, event: ChangeEvent, max_retries: int = 5):
        for attempt in range(max_retries):
            try:
                self._apply_event(cur, event)
                conn.commit()
                return
            except psycopg2.errors.SerializationFailure:
                conn.rollback()
                time.sleep(0.01 * (2 ** attempt))
        raise Exception(f"Failed after {max_retries} retries: {event.lsn}")


# ---------------------------------------------------------------------------
# S3 Archiver
# ---------------------------------------------------------------------------

class S3Archiver:
    def __init__(self, config: CDCConfig):
        self.config = config
        self.s3 = boto3.client("s3")

    def archive_batch(self, events: List[ChangeEvent]) -> Optional[str]:
        if not events or not self.config.s3_bucket:
            return None
        now = datetime.now(timezone.utc)
        partition = now.strftime("year=%Y/month=%m/day=%d/hour=%H")
        batch_id = hashlib.md5(
            f"{events[0].lsn}-{events[-1].lsn}-{now.timestamp()}".encode()
        ).hexdigest()[:12]
        key = f"{self.config.s3_prefix}{partition}/batch_{batch_id}.jsonl"

        lines = []
        for event in events:
            record = {
                "lsn": event.lsn, "timestamp": event.timestamp,
                "schema": event.schema, "table": event.table,
                "operation": event.operation, "primary_keys": event.primary_keys,
                "old_values": event.old_values, "new_values": event.new_values,
                "xid": event.xid, "archived_at": now.isoformat(),
            }
            lines.append(json.dumps(record, default=str))

        self.s3.put_object(
            Bucket=self.config.s3_bucket, Key=key,
            Body="\n".join(lines).encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        logger.info(f"Archived {len(events)} events to s3://{self.config.s3_bucket}/{key}")
        return key

    def send_to_dlq(self, events: List[ChangeEvent], error: str):
        bucket = self.config.dlq_bucket or self.config.s3_bucket
        if not bucket:
            return
        now = datetime.now(timezone.utc)
        key = f"{self.config.s3_prefix}dlq/{now.strftime('%Y/%m/%d/%H')}/failed_{int(now.timestamp())}.jsonl"
        lines = [json.dumps({"event": asdict(e), "error": error}, default=str) for e in events]
        self.s3.put_object(
            Bucket=bucket, Key=key,
            Body="\n".join(lines).encode("utf-8"),
            ContentType="application/x-ndjson",
        )


# ---------------------------------------------------------------------------
# Checkpoint Manager
# ---------------------------------------------------------------------------

class CheckpointManager:
    def __init__(self, config: CDCConfig):
        self.config = config

    def get_last_lsn(self) -> Optional[str]:
        try:
            with psycopg2.connect(self.config.target_dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT last_lsn FROM {self.config.checkpoint_table} WHERE slot_name = %s",
                        (self.config.slot_name,),
                    )
                    row = cur.fetchone()
                    return row[0] if row else None
        except psycopg2.errors.UndefinedTable:
            self._create_checkpoint_table()
            return None

    def save_checkpoint(self, lsn: str):
        with psycopg2.connect(self.config.target_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.config.checkpoint_table} (slot_name, last_lsn, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (slot_name) DO UPDATE
                    SET last_lsn = EXCLUDED.last_lsn, updated_at = NOW()
                    """,
                    (self.config.slot_name, lsn),
                )
            conn.commit()

    def _create_checkpoint_table(self):
        with psycopg2.connect(self.config.target_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.config.checkpoint_table} (
                        slot_name TEXT PRIMARY KEY,
                        last_lsn TEXT NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
            conn.commit()


# ---------------------------------------------------------------------------
# CDC Orchestrator (Lambda polling mode)
# ---------------------------------------------------------------------------

class CDCOrchestrator:
    """Main orchestrator for Lambda polling mode."""

    def __init__(self, config: CDCConfig):
        self.config = config
        self.consumer = WALConsumer(config)
        self.writer = DSQLWriter(config)
        self.archiver = S3Archiver(config)
        self.checkpoint = CheckpointManager(config)
        self.metrics = {
            "events_consumed": 0,
            "events_applied": 0,
            "events_failed": 0,
            "batches_processed": 0,
        }

    def run(self, max_duration_seconds: int = 55) -> dict:
        """Main CDC loop. Runs until max_duration_seconds (Lambda-safe)."""
        start_time = time.time()
        last_lsn = self.checkpoint.get_last_lsn()
        logger.info(f"Starting CDC (polling mode) from LSN: {last_lsn or 'beginning'}")
        logger.info(f"Plugin: {self.config.decoding_plugin.value}")

        try:
            while (time.time() - start_time) < max_duration_seconds:
                events = self.consumer.consume_batch(last_lsn)

                if not events:
                    time.sleep(0.5)
                    continue

                self.metrics["events_consumed"] += len(events)
                batch_last_lsn = events[-1].lsn

                # Apply to DSQL
                success_count, failed_events = self.writer.apply_batch(events)
                self.metrics["events_applied"] += success_count
                self.metrics["events_failed"] += len(failed_events)

                # Handle failures
                if failed_events:
                    self.archiver.send_to_dlq(failed_events, "Failed to apply to DSQL")

                # Archive to S3
                if self.config.s3_bucket:
                    self.archiver.archive_batch(events)

                # Advance slot and checkpoint
                if success_count > 0:
                    self.consumer.advance_slot(batch_last_lsn)
                    self.checkpoint.save_checkpoint(batch_last_lsn)
                    last_lsn = batch_last_lsn

                self.metrics["batches_processed"] += 1

        except Exception as e:
            logger.error(f"CDC loop error: {e}", exc_info=True)
            raise

        elapsed = time.time() - start_time
        self.metrics["runtime_seconds"] = round(elapsed, 2)
        logger.info(f"CDC run complete: {json.dumps(self.metrics)}")
        return self.metrics


# ---------------------------------------------------------------------------
# Lambda Handler
# ---------------------------------------------------------------------------

def lambda_handler(event: dict, context) -> dict:
    """
    AWS Lambda entry point.
    
    Triggered by EventBridge Scheduler, SQS, or direct invocation.
    Event can override config:
      {
        "conflict_mode": "upsert",
        "batch_size": 2000,
        "tables": ["public.orders", "public.customers"],
        "decoding_plugin": "test_decoding"
      }
    """
    config = CDCConfig.from_env()

    # Allow event overrides
    if "conflict_mode" in event:
        config.conflict_mode = ConflictMode(event["conflict_mode"])
    if "batch_size" in event:
        config.batch_size = int(event["batch_size"])
    if "tables" in event:
        config.tables = event["tables"]
    if "decoding_plugin" in event:
        config.decoding_plugin = DecodingPlugin(event["decoding_plugin"])

    # Calculate safe runtime
    remaining_ms = getattr(context, "get_remaining_time_in_millis", lambda: 300000)()
    max_duration = max(10, (remaining_ms // 1000) - 5)

    logger.info(
        f"CDC Lambda starting: plugin={config.decoding_plugin.value}, "
        f"conflict_mode={config.conflict_mode.value}, "
        f"batch_size={config.batch_size}, max_duration={max_duration}s"
    )

    orchestrator = CDCOrchestrator(config)
    metrics = orchestrator.run(max_duration_seconds=max_duration)

    return {"statusCode": 200, "body": json.dumps(metrics)}


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    config = CDCConfig.from_env()

    if len(sys.argv) > 1:
        config.conflict_mode = ConflictMode(sys.argv[1])

    print(f"Starting CDC (polling mode): {config.source_dsn[:30]}... → DSQL")
    print(f"Plugin: {config.decoding_plugin.value}")
    print(f"Conflict mode: {config.conflict_mode.value}")
    print(f"Tables: {config.tables or 'ALL'}")

    orchestrator = CDCOrchestrator(config)

    while True:
        try:
            metrics = orchestrator.run(max_duration_seconds=60)
            print(f"Cycle complete: {metrics}")
        except KeyboardInterrupt:
            print("\nShutdown requested")
            break
        except Exception as e:
            print(f"Error: {e}, retrying in 5s...")
            time.sleep(5)
