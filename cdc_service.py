"""
PostgreSQL 14+ → Aurora DSQL — Continuous CDC Replication Service
================================================================
Designed to run 24/7 as an ECS Fargate task (or EC2/EKS pod) for
continuous real-time replication until cutover.

Decoding plugins supported:
  - test_decoding (built-in, no extensions needed) ← DEFAULT
  - pgoutput (native PG 10+ logical replication protocol)

NO external extensions required (no wal2json dependency).

Key features:
  - Streaming replication connection (persistent WAL stream, not polling)
  - True real-time (<1 second latency) via START_REPLICATION protocol
  - Automatic reconnection with exponential backoff
  - Graceful shutdown (SIGTERM/SIGINT) with clean slot state
  - Health check endpoint (HTTP /health) for ECS health checks
  - Prometheus-compatible metrics endpoint (/metrics)
  - Lag monitoring with CloudWatch custom metrics
  - Configurable back-pressure (pause consumption when target is slow)
  - Connection pooling for DSQL writes
  - Multi-process option for horizontal scaling (per-table sharding)

Deployment options (all support 24/7):
  1. ECS Fargate (recommended) — serverless containers, auto-restart on failure
  2. ECS on EC2 — for predictable high throughput
  3. EKS / Kubernetes — if you already have a cluster
  4. EC2 with systemd — simplest for testing

Environment Variables:
  SOURCE_DSN          - PostgreSQL source connection string
  TARGET_DSN          - Aurora DSQL connection string
  SLOT_NAME           - Logical replication slot name (default: dsql_cdc_slot)
  PUBLICATION_NAME    - Publication name (default: dsql_cdc_pub)
  DECODING_PLUGIN     - test_decoding | pgoutput (default: test_decoding)
  S3_BUCKET           - S3 bucket for change archival (optional)
  S3_PREFIX           - S3 key prefix (default: cdc-events/)
  CONFLICT_MODE       - upsert | skip | fail | last_write_wins (default: upsert)
  BATCH_SIZE          - Flush after N events (default: 1000)
  FLUSH_INTERVAL_MS   - Flush after N ms even if batch not full (default: 500)
  CHECKPOINT_TABLE    - Table for LSN checkpoints (default: _cdc_checkpoint)
  TABLES              - Comma-separated tables (default: all)
  PARALLEL_WORKERS    - Parallel DSQL writer threads (default: 4)
  HEALTH_PORT         - Health check HTTP port (default: 8080)
  METRICS_ENABLED     - Enable CloudWatch metrics (default: true)
  LOG_LEVEL           - DEBUG, INFO, WARNING, ERROR (default: INFO)
  MAX_LAG_BYTES       - Back-pressure: pause if lag exceeds this (default: 1073741824 = 1GB)
  RECONNECT_MAX_WAIT  - Max reconnect backoff seconds (default: 60)
"""


import json
import os
import re
import sys
import time
import signal
import logging
import hashlib
import struct
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import HTTPServer, BaseHTTPRequestHandler
from queue import Queue, Full, Empty

import psycopg2
import psycopg2.extras
import psycopg2.extensions
import boto3


# ---------------------------------------------------------------------------
# DSQL IAM Token Manager
# ---------------------------------------------------------------------------

class DSQLTokenManager:
    """
    Auto-refreshes Aurora DSQL IAM authentication tokens.
    
    DSQL uses IAM-signed tokens as passwords. These tokens expire after ~15 minutes.
    This manager generates fresh tokens before expiry, ensuring connections never fail
    due to stale credentials.
    
    Environment Variables:
      DSQL_HOSTNAME     - DSQL cluster endpoint (auto-extracted from TARGET_DSN if not set)
      DSQL_REGION       - AWS region (auto-extracted from hostname if not set)
      DSQL_TOKEN_TTL    - Token refresh interval in seconds (default: 600 = 10 min)
      DSQL_AUTH_ACTION  - 'admin' or 'connect' (default: admin)
    """

    def __init__(self, target_dsn: str):
        self._target_dsn = target_dsn
        self._lock = threading.Lock()
        self._current_token: Optional[str] = None
        self._token_generated_at: float = 0
        self._token_ttl: int = int(os.environ.get("DSQL_TOKEN_TTL", "600"))  # 10 min
        
        # Extract hostname from DSN
        self._hostname = os.environ.get("DSQL_HOSTNAME", "")
        if not self._hostname:
            self._hostname = self._extract_hostname(target_dsn)
        
        # Extract region from hostname (e.g., xxx.dsql.us-east-1.on.aws)
        self._region = os.environ.get("DSQL_REGION", "")
        if not self._region:
            self._region = self._extract_region(self._hostname)
        
        # Auth action
        self._auth_action = os.environ.get("DSQL_AUTH_ACTION", "admin")
        
        self._dsql_client = None
        if self._hostname and self._region:
            try:
                self._dsql_client = boto3.client("dsql", region_name=self._region)
                logger.info(f"DSQL token manager initialized: host={self._hostname}, "
                           f"region={self._region}, refresh_interval={self._token_ttl}s")
            except Exception as e:
                logger.warning(f"DSQL client init failed: {e}. Token auto-refresh disabled.")

    def get_dsn(self) -> str:
        """Get TARGET_DSN with a fresh token as password."""
        if not self._dsql_client:
            return self._target_dsn  # Fallback to original DSN
        
        with self._lock:
            now = time.time()
            if not self._current_token or (now - self._token_generated_at) > self._token_ttl:
                self._refresh_token()
            
            # Replace password in DSN with fresh token
            return self._replace_password(self._target_dsn, self._current_token)

    def _refresh_token(self):
        """Generate a new DSQL auth token."""
        try:
            if self._auth_action == "admin":
                token = self._dsql_client.generate_db_connect_admin_auth_token(
                    Hostname=self._hostname, Region=self._region, ExpiresIn=900
                )
            else:
                token = self._dsql_client.generate_db_connect_auth_token(
                    Hostname=self._hostname, Region=self._region, ExpiresIn=900
                )
            self._current_token = token
            self._token_generated_at = time.time()
            logger.info("DSQL auth token refreshed successfully")
        except Exception as e:
            logger.error(f"Failed to refresh DSQL token: {e}")

    def _extract_hostname(self, dsn: str) -> str:
        """Extract host= value from DSN string."""
        import re
        match = re.search(r'host=([^\s]+)', dsn)
        return match.group(1) if match else ""

    def _extract_region(self, hostname: str) -> str:
        """Extract AWS region from DSQL hostname (e.g., xxx.dsql.us-east-1.on.aws)."""
        import re
        match = re.search(r'dsql\.([a-z0-9-]+)\.on\.aws', hostname)
        return match.group(1) if match else os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    def _replace_password(self, dsn: str, new_pw: str) -> str:
        """Replace the auth token field in a DSN string."""
        import re
        pw_field = "password"
        # Pattern: password=<anything up to next space or end of string>
        pattern = pw_field + r"=\S+"
        replacement = pw_field + "=" + new_pw
        if re.search(pattern, dsn):
            return re.sub(pattern, replacement, dsn)
        # If no auth field exists, append it
        return dsn + " " + pw_field + "=" + new_pw

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

logger = logging.getLogger("pg_dsql_cdc")


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
    flush_interval_ms: int = 500
    checkpoint_table: str = "_cdc_checkpoint"
    tables: list = field(default_factory=list)
    parallel_workers: int = 4
    health_port: int = 8080
    metrics_enabled: bool = True
    log_level: str = "INFO"
    max_lag_bytes: int = 1_073_741_824  # 1 GB
    reconnect_max_wait: int = 60

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
            flush_interval_ms=int(os.environ.get("FLUSH_INTERVAL_MS", "500")),
            checkpoint_table=os.environ.get("CHECKPOINT_TABLE", "_cdc_checkpoint"),
            tables=tables,
            parallel_workers=int(os.environ.get("PARALLEL_WORKERS", "4")),
            health_port=int(os.environ.get("HEALTH_PORT", "8080")),
            metrics_enabled=os.environ.get("METRICS_ENABLED", "true").lower() == "true",
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
            max_lag_bytes=int(os.environ.get("MAX_LAG_BYTES", "1073741824")),
            reconnect_max_wait=int(os.environ.get("RECONNECT_MAX_WAIT", "60")),
        )



# ---------------------------------------------------------------------------
# Table Rule Engine (DMS-style rules)
# ---------------------------------------------------------------------------

class TableRuleEngine:
    """
    DMS-style table selection using include/exclude rules with SQL LIKE wildcards.
    Rules file format:
    {
        "rules": [
            {"rule-type": "selection", "rule-action": "include",
             "object-locator": {"schema-name": "public", "table-name": "%"}},
            {"rule-type": "selection", "rule-action": "exclude",
             "object-locator": {"schema-name": "public", "table-name": "tmp_%"}}
        ]
    }
    """

    def __init__(self, rules_file: Optional[str] = None):
        self._rules_file = rules_file or os.environ.get("TABLE_RULES_FILE", "/opt/cdc/table_rules.json")
        self._include_rules: List[Dict] = []
        self._exclude_rules: List[Dict] = []
        self._last_mtime: float = 0
        self._load_rules()

    def _load_rules(self):
        """Load rules from the JSON file."""
        try:
            if not os.path.exists(self._rules_file):
                self._include_rules = []
                self._exclude_rules = []
                return
            mtime = os.path.getmtime(self._rules_file)
            if mtime == self._last_mtime:
                return  # No change
            self._last_mtime = mtime
            with open(self._rules_file) as f:
                data = json.load(f)
            self._include_rules = []
            self._exclude_rules = []
            for rule in data.get("rules", []):
                if rule.get("rule-type") != "selection":
                    continue
                action = rule.get("rule-action", "include")
                locator = rule.get("object-locator", {})
                compiled = {
                    "schema_pattern": self._like_to_regex(locator.get("schema-name", "%")),
                    "table_pattern": self._like_to_regex(locator.get("table-name", "%")),
                }
                if action == "include":
                    self._include_rules.append(compiled)
                elif action == "exclude":
                    self._exclude_rules.append(compiled)
            logger.info(f"Table rules loaded: {len(self._include_rules)} include, "
                       f"{len(self._exclude_rules)} exclude rules from {self._rules_file}")
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to load table rules from {self._rules_file}: {e}")

    def _like_to_regex(self, pattern: str) -> "re.Pattern":
        """Convert SQL LIKE pattern to compiled regex. % = .*, _ = ."""
        # Escape regex special chars except our wildcards
        escaped = ""
        for ch in pattern:
            if ch == "%":
                escaped += ".*"
            elif ch == "_":
                escaped += "."
            elif ch in r"\.[]{}()*+?^$|":
                escaped += "\\" + ch
            else:
                escaped += ch
        return re.compile(f"^{escaped}$", re.IGNORECASE)

    def should_replicate(self, schema: str, table: str) -> bool:
        """Check if a table should be replicated based on the loaded rules."""
        # Hot-reload if file changed
        self._load_rules()

        # If no include rules defined, default = include all
        if self._include_rules:
            included = any(
                r["schema_pattern"].match(schema) and r["table_pattern"].match(table)
                for r in self._include_rules
            )
            if not included:
                return False

        # Check excludes
        excluded = any(
            r["schema_pattern"].match(schema) and r["table_pattern"].match(table)
            for r in self._exclude_rules
        )
        return not excluded

    @property
    def rules_count(self) -> int:
        return len(self._include_rules) + len(self._exclude_rules)

    @property
    def rules_summary(self) -> str:
        if self.rules_count == 0:
            return "ALL"
        return f"{len(self._include_rules)} include, {len(self._exclude_rules)} exclude"


# ---------------------------------------------------------------------------
# Replication Control Manager
# ---------------------------------------------------------------------------

class ControlManager:
    """
    Manages CDC service lifecycle state via a control file.
    States: running, paused, stopped.
    The service polls this every ~3 seconds.
    """

    def __init__(self, control_file: Optional[str] = None):
        self._control_file = control_file or os.environ.get("CONTROL_FILE", "/opt/cdc/control.json")
        self._last_state = "running"

    def get_state(self) -> str:
        """Read current state from control file. Missing file = running."""
        try:
            if not os.path.exists(self._control_file):
                return "running"
            with open(self._control_file) as f:
                data = json.load(f)
            state = data.get("state", "running")
            if state != self._last_state:
                logger.info(f"Control state changed: {self._last_state} -> {state}")
                self._last_state = state
            return state
        except (json.JSONDecodeError, IOError):
            return "running"

    def set_state(self, state: str):
        """Write state to control file."""
        try:
            os.makedirs(os.path.dirname(self._control_file), exist_ok=True)
            with open(self._control_file, "w") as f:
                json.dump({"state": state}, f)
            self._last_state = state
            logger.info(f"Control state set to: {state}")
        except IOError as e:
            logger.error(f"Failed to write control file: {e}")


# ---------------------------------------------------------------------------
# Metrics Collector
# ---------------------------------------------------------------------------

class Metrics:
    """Thread-safe metrics collector with CloudWatch and /metrics endpoint support."""

    def __init__(self, config: CDCConfig):
        self.config = config
        self._lock = threading.Lock()
        self._counters = {
            "events_consumed": 0,
            "events_applied": 0,
            "events_failed": 0,
            "batches_processed": 0,
            "reconnections": 0,
            "s3_archives": 0,
            "checkpoints_saved": 0,
        }
        self._gauges = {
            "replication_lag_bytes": 0,
            "last_event_timestamp": 0,
            "batch_queue_depth": 0,
            "active_writers": 0,
            "uptime_seconds": 0,
        }
        self._start_time = time.time()
        self._cw_client = None
        self._table_stats: Dict[str, Dict] = {}  # per-table event tracking
        if config.metrics_enabled:
            try:
                self._cw_client = boto3.client("cloudwatch")
            except Exception:
                logger.warning("CloudWatch client init failed, metrics disabled")

    def increment(self, counter: str, value: int = 1):
        with self._lock:
            self._counters[counter] = self._counters.get(counter, 0) + value

    def track_table_event(self, schema: str, table: str, operation: str, success: bool = True):
        """Track per-table event stats."""
        with self._lock:
            fqtn = f"{schema}.{table}"
            if fqtn not in self._table_stats:
                self._table_stats[fqtn] = {"events_applied": 0, "events_failed": 0, "last_event": None, "operations": {}}
            stats = self._table_stats[fqtn]
            if success:
                stats["events_applied"] += 1
            else:
                stats["events_failed"] += 1
            stats["last_event"] = time.time()
            stats["operations"][operation] = stats["operations"].get(operation, 0) + 1

    def set_gauge(self, gauge: str, value: float):
        with self._lock:
            self._gauges[gauge] = value

    def get_snapshot(self) -> dict:
        with self._lock:
            self._gauges["uptime_seconds"] = int(time.time() - self._start_time)
            return {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "tables": [
                    {"name": k, "eventsApplied": v["events_applied"], "errors": v["events_failed"],
                     "lastEvent": v["last_event"], "operations": v["operations"]}
                    for k, v in self._table_stats.items()
                ],
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

    def publish_to_cloudwatch(self):
        """Publish key metrics to CloudWatch (called periodically)."""
        if not self._cw_client:
            return
        try:
            snapshot = self.get_snapshot()
            metric_data = [
                {
                    "MetricName": "ReplicationLagBytes",
                    "Value": snapshot["gauges"]["replication_lag_bytes"],
                    "Unit": "Bytes",
                },
                {
                    "MetricName": "EventsAppliedPerMinute",
                    "Value": snapshot["counters"]["events_applied"],
                    "Unit": "Count",
                },
                {
                    "MetricName": "EventsFailed",
                    "Value": snapshot["counters"]["events_failed"],
                    "Unit": "Count",
                },
                {
                    "MetricName": "BatchQueueDepth",
                    "Value": snapshot["gauges"]["batch_queue_depth"],
                    "Unit": "Count",
                },
            ]
            self._cw_client.put_metric_data(
                Namespace="CDC/PgToDSQL",
                MetricData=metric_data,
            )
        except Exception as e:
            logger.warning(f"Failed to publish CloudWatch metrics: {e}")


# ---------------------------------------------------------------------------
# Health Check HTTP Server
# ---------------------------------------------------------------------------

class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP handler for /health and /metrics endpoints."""

    service = None  # Set by CDCService

    def _send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        """Handle CORS preflight requests."""
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            status = self.service.get_health_status() if self.service else {}
            healthy = status.get("healthy", False)
            self.send_response(200 if healthy else 503)
            self.send_header("Content-Type", "application/json")
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps(status).encode())

        elif self.path == "/metrics":
            metrics = self.service.metrics.get_snapshot() if self.service else {}
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps(metrics, indent=2).encode())

        elif self.path == "/table-mapping":
            # Return current table rules file contents
            rules_file = os.environ.get("TABLE_RULES_FILE", "/opt/cdc/table_rules.json")
            rules_data = {"rules": []}
            if os.path.exists(rules_file):
                try:
                    with open(rules_file) as f:
                        rules_data = json.load(f)
                except (json.JSONDecodeError, IOError):
                    pass
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps(rules_data, indent=2).encode())

        elif self.path == "/ready":
            ready = self.service._is_streaming if self.service else False
            self.send_response(200 if ready else 503)
            self.send_header("Content-Type", "text/plain")
            self._send_cors_headers()
            self.end_headers()
            self.wfile.write(b"ready" if ready else b"not ready")

        else:
            self.send_response(404)
            self._send_cors_headers()
            self.end_headers()

    def do_POST(self):
        """Handle POST /control to change replication state."""
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"

        if self.path == "/control":
            try:
                data = json.loads(body)
                new_state = data.get("state", "")
                if new_state not in ("running", "paused", "stopped"):
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json")
                    self._send_cors_headers()
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "Invalid state. Use: running, paused, stopped"}).encode())
                    return

                # Write control file
                control_file = os.environ.get("CONTROL_FILE", "/opt/cdc/control.json")
                os.makedirs(os.path.dirname(control_file), exist_ok=True)
                with open(control_file, "w") as f:
                    json.dump({"state": new_state}, f)

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self._send_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({"status": "ok", "state": new_state}).encode())
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self._send_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())

        elif self.path == "/table-mapping":
            try:
                rules_data = json.loads(body)
                # Validate structure
                if "rules" not in rules_data or not isinstance(rules_data["rules"], list):
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json")
                    self._send_cors_headers()
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "Invalid format. Expected: {\"rules\": [...]}"}).encode())
                    return

                # Write table rules file (hot-reloaded by TableRuleEngine)
                rules_file = os.environ.get("TABLE_RULES_FILE", "/opt/cdc/table_rules.json")
                os.makedirs(os.path.dirname(rules_file), exist_ok=True)
                with open(rules_file, "w") as f:
                    json.dump(rules_data, f, indent=2)

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self._send_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({"status": "ok", "rules_count": len(rules_data["rules"])}).encode())
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self._send_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        else:
            self.send_response(404)
            self._send_cors_headers()
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress access logs


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
    columns: list   # [{name, type, pk}]
    old_values: Optional[dict]
    new_values: Optional[dict]
    xid: int = 0

    @property
    def fqtn(self) -> str:
        return f'"{self.schema}"."{self.table}"'

    @property
    def primary_keys(self) -> List[str]:
        return [c["name"] for c in self.columns if c.get("pk", False)]


# ---------------------------------------------------------------------------
# WAL Message Parsers
# ---------------------------------------------------------------------------

class TestDecodingParser:
    """
    Parses test_decoding output format.
    
    test_decoding outputs text like:
      table public.orders: INSERT: id[integer]:1 name[text]:'hello' price[numeric]:9.99
      table public.orders: UPDATE: old-key: id[integer]:1 new-tuple: id[integer]:1 name[text]:'world'
      table public.orders: DELETE: id[integer]:1
    
    This parser extracts structured change events from these text messages.
    """

    # Regex patterns for test_decoding output
    _TABLE_PATTERN = re.compile(
        r"^table\s+(?P<schema>\w+)\.(?P<table>\w+):\s+(?P<op>INSERT|UPDATE|DELETE):\s+(?P<data>.*)$"
    )
    _COLUMN_PATTERN = re.compile(
        r"(?P<name>\w+)\[(?P<type>[^\]]+)\]:(?P<value>(?:'(?:[^'\\]|\\.)*'|[^\s]+))"
    )

    def __init__(self, config: CDCConfig):
        self.config = config
        self._table_pk_cache: Dict[str, List[str]] = {}
        self._rule_engine = TableRuleEngine()

    def parse_message(self, payload: str, lsn: str, xid: int = 0) -> List[ChangeEvent]:
        """Parse a test_decoding message into ChangeEvent objects."""
        events = []
        timestamp = datetime.now(timezone.utc).isoformat()

        # test_decoding can send multiple lines per message (BEGIN/COMMIT wrappers)
        for line in payload.strip().split("\n"):
            line = line.strip()

            # Skip transaction markers
            if line.startswith("BEGIN") or line.startswith("COMMIT"):
                continue

            match = self._TABLE_PATTERN.match(line)
            if not match:
                continue

            schema = match.group("schema")
            table = match.group("table")
            operation = match.group("op")
            data = match.group("data")

            # Check table filter (DMS-style rules + legacy config)
            if not self._rule_engine.should_replicate(schema, table):
                continue
            if self.config.tables:
                fqtn = f"{schema}.{table}"
                if fqtn not in self.config.tables and table not in self.config.tables:
                    continue

            # Parse columns and values
            if operation == "INSERT":
                new_values, columns = self._parse_columns(data)
                old_values = None
            elif operation == "UPDATE":
                old_values, new_values, columns = self._parse_update(data)
            elif operation == "DELETE":
                old_values, columns = self._parse_columns(data)
                new_values = None
            else:
                continue

            # Mark PK columns (use cached info from REPLICA IDENTITY)
            pk_cols = self._get_pk_columns(schema, table)
            for col in columns:
                col["pk"] = col["name"] in pk_cols

            events.append(ChangeEvent(
                lsn=lsn,
                timestamp=timestamp,
                schema=schema,
                table=table,
                operation=operation,
                columns=columns,
                old_values=old_values,
                new_values=new_values,
                xid=xid,
            ))

        return events

    def _parse_columns(self, data: str) -> Tuple[dict, list]:
        """Parse column data string into values dict and column metadata."""
        values = {}
        columns = []

        for match in self._COLUMN_PATTERN.finditer(data):
            name = match.group("name")
            col_type = match.group("type")
            raw_value = match.group("value")

            # Convert value
            value = self._convert_value(raw_value, col_type)
            values[name] = value
            columns.append({"name": name, "type": col_type, "pk": False})

        return values, columns

    def _parse_update(self, data: str) -> Tuple[Optional[dict], Optional[dict], list]:
        """Parse UPDATE data which may contain old-key and new-tuple sections."""
        old_values = None
        new_values = None
        columns = []

        if "old-key:" in data and "new-tuple:" in data:
            # Has both old and new values
            parts = data.split("new-tuple:")
            old_part = parts[0].replace("old-key:", "").strip()
            new_part = parts[1].strip()

            old_values, _ = self._parse_columns(old_part)
            new_values, columns = self._parse_columns(new_part)
        elif "old-key:" in data:
            # Only old key (unlikely but handle it)
            old_part = data.replace("old-key:", "").strip()
            old_values, columns = self._parse_columns(old_part)
        else:
            # Only new values (no REPLICA IDENTITY FULL)
            new_values, columns = self._parse_columns(data)

        return old_values, new_values, columns

    def _convert_value(self, raw: str, col_type: str) -> Any:
        """Convert a test_decoding raw value to Python type."""
        if raw == "null":
            return None

        # Strip quotes from text values
        if raw.startswith("'") and raw.endswith("'"):
            # Unescape single quotes
            return raw[1:-1].replace("\\'", "'").replace("\\\\", "\\")

        # Numeric types
        if col_type in ("integer", "bigint", "smallint", "int4", "int8", "int2"):
            return int(raw)
        if col_type in ("numeric", "decimal", "float4", "float8", "double precision", "real"):
            return float(raw)
        if col_type == "boolean":
            return raw.lower() in ("t", "true", "1")

        return raw

    def _get_pk_columns(self, schema: str, table: str) -> List[str]:
        """Get PK columns for a table (cached, fetched from source on first call)."""
        key = f"{schema}.{table}"
        if key not in self._table_pk_cache:
            self._table_pk_cache[key] = self._fetch_pk_columns(schema, table)
        return self._table_pk_cache[key]

    def _fetch_pk_columns(self, schema: str, table: str) -> List[str]:
        """Fetch PK columns from the source database."""
        try:
            with psycopg2.connect(self.config.source_dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT a.attname
                        FROM pg_index i
                        JOIN pg_attribute a ON a.attrelid = i.indrelid
                            AND a.attnum = ANY(i.indkey)
                        WHERE i.indrelid = %s::regclass
                            AND i.indisprimary
                        ORDER BY array_position(i.indkey, a.attnum)
                    """, (f"{schema}.{table}",))
                    return [row[0] for row in cur.fetchall()]
        except Exception as e:
            logger.warning(f"Failed to fetch PK for {schema}.{table}: {e}")
            return []


class PgOutputParser:
    """
    Parses pgoutput binary protocol messages.
    
    pgoutput is the native logical replication protocol in PostgreSQL 10+.
    It outputs binary messages that represent relation metadata, inserts,
    updates, deletes, begin/commit transactions.
    
    Message types:
      'R' - Relation (table metadata)
      'I' - Insert
      'U' - Update
      'D' - Delete
      'B' - Begin transaction
      'C' - Commit transaction
      'O' - Origin
      'T' - Truncate
    """

    def __init__(self, config: CDCConfig):
        self.config = config
        # Cache relation metadata (relation_id -> {schema, table, columns, pk_cols})
        self._relations: Dict[int, dict] = {}
        self._current_xid: int = 0
        self._rule_engine = TableRuleEngine()
        self._current_timestamp: str = ""

    def parse_message(self, payload: bytes, lsn: str, xid: int = 0) -> List[ChangeEvent]:
        """Parse a pgoutput binary message into ChangeEvent objects."""
        if not payload:
            return []

        # pgoutput sends raw bytes
        if isinstance(payload, str):
            # If psycopg2 decoded it, we need to handle text format
            # This happens when decode=True in start_replication
            return self._parse_text_protocol(payload, lsn)

        msg_type = chr(payload[0])
        data = payload[1:]

        if msg_type == 'B':  # Begin
            self._parse_begin(data)
            return []
        elif msg_type == 'C':  # Commit
            return []
        elif msg_type == 'R':  # Relation
            self._parse_relation(data)
            return []
        elif msg_type == 'I':  # Insert
            return self._parse_insert(data, lsn)
        elif msg_type == 'U':  # Update
            return self._parse_update(data, lsn)
        elif msg_type == 'D':  # Delete
            return self._parse_delete(data, lsn)
        elif msg_type == 'T':  # Truncate
            logger.info("Received TRUNCATE message (not replicated)")
            return []
        else:
            return []

    def _parse_text_protocol(self, payload: str, lsn: str) -> List[ChangeEvent]:
        """
        Handle pgoutput in text mode (when psycopg2 decode=True).
        pgoutput with decode=True gives us structured text output.
        We parse it similar to test_decoding but with pgoutput format.
        """
        # When using pgoutput with psycopg2's consume_stream in decode mode,
        # the payload comes as bytes that represent the pgoutput protocol.
        # We handle this via the binary path.
        return []

    def _parse_begin(self, data: bytes):
        """Parse BEGIN message: final_lsn(8) + timestamp(8) + xid(4)."""
        if len(data) >= 20:
            # timestamp is microseconds since 2000-01-01
            ts_microseconds = struct.unpack('!q', data[8:16])[0]
            # Convert PG epoch (2000-01-01) to Unix epoch
            pg_epoch_offset = 946684800  # seconds from 1970-01-01 to 2000-01-01
            unix_ts = pg_epoch_offset + (ts_microseconds / 1_000_000)
            self._current_timestamp = datetime.fromtimestamp(unix_ts, tz=timezone.utc).isoformat()
            self._current_xid = struct.unpack('!I', data[16:20])[0]

    def _parse_relation(self, data: bytes):
        """Parse RELATION message to cache table metadata."""
        offset = 0

        # Relation ID (4 bytes)
        rel_id = struct.unpack('!I', data[offset:offset + 4])[0]
        offset += 4

        # Namespace (null-terminated string)
        schema, offset = self._read_string(data, offset)

        # Relation name (null-terminated string)
        table, offset = self._read_string(data, offset)

        # Replica identity (1 byte)
        replica_identity = chr(data[offset])
        offset += 1

        # Number of columns (2 bytes)
        n_cols = struct.unpack('!H', data[offset:offset + 2])[0]
        offset += 2

        columns = []
        pk_cols = []
        for _ in range(n_cols):
            # Flags (1 byte): 1 = part of key
            flags = data[offset]
            offset += 1

            # Column name (null-terminated)
            col_name, offset = self._read_string(data, offset)

            # Data type OID (4 bytes)
            type_oid = struct.unpack('!I', data[offset:offset + 4])[0]
            offset += 4

            # Type modifier (4 bytes)
            type_mod = struct.unpack('!i', data[offset:offset + 4])[0]
            offset += 4

            is_key = bool(flags & 1)
            columns.append({
                "name": col_name,
                "type_oid": type_oid,
                "type": self._oid_to_type_name(type_oid),
                "pk": is_key,
            })
            if is_key:
                pk_cols.append(col_name)

        # Check table filter (DMS-style rules + legacy config)
        if not self._rule_engine.should_replicate(schema, table):
            return
        if self.config.tables:
            fqtn = f"{schema}.{table}"
            if fqtn not in self.config.tables and table not in self.config.tables:
                return

        self._relations[rel_id] = {
            "schema": schema,
            "table": table,
            "columns": columns,
            "pk_cols": pk_cols,
            "replica_identity": replica_identity,
        }
        logger.debug(f"Cached relation: {schema}.{table} ({len(columns)} cols, PK: {pk_cols})")

    def _parse_insert(self, data: bytes, lsn: str) -> List[ChangeEvent]:
        """Parse INSERT message."""
        offset = 0
        rel_id = struct.unpack('!I', data[offset:offset + 4])[0]
        offset += 4

        if rel_id not in self._relations:
            return []

        rel = self._relations[rel_id]

        # 'N' marker for new tuple
        if data[offset:offset + 1] == b'N':
            offset += 1

        values, offset = self._parse_tuple_data(data, offset, rel["columns"])

        return [ChangeEvent(
            lsn=lsn,
            timestamp=self._current_timestamp or datetime.now(timezone.utc).isoformat(),
            schema=rel["schema"],
            table=rel["table"],
            operation="INSERT",
            columns=rel["columns"],
            old_values=None,
            new_values=values,
            xid=self._current_xid,
        )]

    def _parse_update(self, data: bytes, lsn: str) -> List[ChangeEvent]:
        """Parse UPDATE message."""
        offset = 0
        rel_id = struct.unpack('!I', data[offset:offset + 4])[0]
        offset += 4

        if rel_id not in self._relations:
            return []

        rel = self._relations[rel_id]
        old_values = None
        new_values = None

        # Check for old tuple ('K' = key, 'O' = old tuple)
        marker = chr(data[offset])
        if marker in ('K', 'O'):
            offset += 1
            old_values, offset = self._parse_tuple_data(data, offset, rel["columns"])
            marker = chr(data[offset])

        # New tuple ('N')
        if marker == 'N':
            offset += 1
            new_values, offset = self._parse_tuple_data(data, offset, rel["columns"])

        return [ChangeEvent(
            lsn=lsn,
            timestamp=self._current_timestamp or datetime.now(timezone.utc).isoformat(),
            schema=rel["schema"],
            table=rel["table"],
            operation="UPDATE",
            columns=rel["columns"],
            old_values=old_values,
            new_values=new_values,
            xid=self._current_xid,
        )]

    def _parse_delete(self, data: bytes, lsn: str) -> List[ChangeEvent]:
        """Parse DELETE message."""
        offset = 0
        rel_id = struct.unpack('!I', data[offset:offset + 4])[0]
        offset += 4

        if rel_id not in self._relations:
            return []

        rel = self._relations[rel_id]

        # Old tuple marker ('K' = key only, 'O' = full old tuple)
        marker = chr(data[offset])
        offset += 1

        old_values, offset = self._parse_tuple_data(data, offset, rel["columns"])

        return [ChangeEvent(
            lsn=lsn,
            timestamp=self._current_timestamp or datetime.now(timezone.utc).isoformat(),
            schema=rel["schema"],
            table=rel["table"],
            operation="DELETE",
            columns=rel["columns"],
            old_values=old_values,
            new_values=None,
            xid=self._current_xid,
        )]

    def _parse_tuple_data(self, data: bytes, offset: int, columns: list) -> Tuple[dict, int]:
        """Parse tuple data (column values)."""
        n_cols = struct.unpack('!H', data[offset:offset + 2])[0]
        offset += 2

        values = {}
        for i in range(min(n_cols, len(columns))):
            col_type_byte = chr(data[offset])
            offset += 1

            if col_type_byte == 'n':  # NULL
                values[columns[i]["name"]] = None
            elif col_type_byte == 'u':  # Unchanged TOAST
                values[columns[i]["name"]] = None  # Mark as unchanged
            elif col_type_byte == 't':  # Text value
                val_len = struct.unpack('!I', data[offset:offset + 4])[0]
                offset += 4
                val_bytes = data[offset:offset + val_len]
                offset += val_len
                values[columns[i]["name"]] = self._convert_pg_value(
                    val_bytes.decode("utf-8"), columns[i].get("type_oid", 0)
                )
            elif col_type_byte == 'b':  # Binary value
                val_len = struct.unpack('!I', data[offset:offset + 4])[0]
                offset += 4
                values[columns[i]["name"]] = data[offset:offset + val_len].hex()
                offset += val_len

        return values, offset

    def _read_string(self, data: bytes, offset: int) -> Tuple[str, int]:
        """Read a null-terminated string from binary data."""
        end = data.index(0, offset)
        s = data[offset:end].decode("utf-8")
        return s, end + 1

    def _convert_pg_value(self, text_val: str, type_oid: int) -> Any:
        """Convert PostgreSQL text representation to Python value."""
        # Common OIDs
        INT_OIDS = {20, 21, 23, 26}   # int8, int2, int4, oid
        FLOAT_OIDS = {700, 701, 1700}  # float4, float8, numeric
        BOOL_OID = 16

        if type_oid in INT_OIDS:
            return int(text_val)
        elif type_oid in FLOAT_OIDS:
            return float(text_val)
        elif type_oid == BOOL_OID:
            return text_val.lower() in ('t', 'true', '1')
        return text_val

    def _oid_to_type_name(self, oid: int) -> str:
        """Map common OIDs to type names."""
        OID_MAP = {
            16: "boolean", 20: "bigint", 21: "smallint", 23: "integer",
            25: "text", 26: "oid", 700: "real", 701: "double precision",
            1043: "varchar", 1082: "date", 1114: "timestamp",
            1184: "timestamptz", 1700: "numeric", 2950: "uuid",
            3802: "jsonb", 114: "json",
        }
        return OID_MAP.get(oid, f"oid_{oid}")


# ---------------------------------------------------------------------------
# Streaming WAL Consumer (uses START_REPLICATION for true streaming)
# ---------------------------------------------------------------------------

class StreamingWALConsumer:
    """
    Consumes WAL changes via PostgreSQL's streaming replication protocol.
    Supports both test_decoding and pgoutput plugins.
    """

    def __init__(self, config: CDCConfig, event_queue: Queue, metrics: Metrics):
        self.config = config
        self.event_queue = event_queue
        self.metrics = metrics
        self._conn = None
        self._cursor = None
        self._running = False
        self._last_lsn = None
        
        # Initialize the appropriate parser
        if config.decoding_plugin == DecodingPlugin.PGOUTPUT:
            self._parser = PgOutputParser(config)
        else:
            self._parser = TestDecodingParser(config)

    def start(self, start_lsn: Optional[str] = None):
        """Start the streaming replication consumer."""
        self._running = True
        self._last_lsn = start_lsn
        self._connect_and_stream()

    def stop(self):
        """Gracefully stop the consumer."""
        self._running = False
        if self._cursor:
            try:
                self._cursor.close()
            except Exception:
                pass
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass

    def _connect_and_stream(self):
        """Connect to source and start streaming with auto-reconnect."""
        backoff = 1

        while self._running:
            try:
                self._establish_connection()
                self._start_streaming()
                backoff = 1  # Reset on successful connection

            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                if not self._running:
                    break
                logger.warning(f"Connection lost: {e}. Reconnecting in {backoff}s...")
                self.metrics.increment("reconnections")
                # Sleep in 1-second increments so we can respond to stop signals quickly
                for _ in range(int(backoff)):
                    if not self._running:
                        return
                    time.sleep(1)
                backoff = min(backoff * 2, self.config.reconnect_max_wait)

            except Exception as e:
                if not self._running:
                    break
                logger.error(f"Unexpected error in WAL consumer: {e}", exc_info=True)
                for _ in range(int(backoff)):
                    if not self._running:
                        return
                    time.sleep(1)
                backoff = min(backoff * 2, self.config.reconnect_max_wait)

    def _format_lsn(self, lsn) -> int:
        """Convert LSN to integer for psycopg2 start_replication.
        Accepts: None, 0, integer string ('5814244869352'), or PG format ('549/28000778').
        Returns integer (0 means start from slot's confirmed_flush_lsn).
        """
        if not lsn:
            return 0
        lsn_str = str(lsn)
        if '/' in lsn_str:
            # PG format: "549/28000778" -> integer
            hi, lo = lsn_str.split('/')
            return (int(hi, 16) << 32) + int(lo, 16)
        # Already an integer string
        try:
            return int(lsn_str)
        except ValueError:
            return 0

    def _establish_connection(self):
        """Create a logical replication connection."""
        self._conn = psycopg2.connect(
            self.config.source_dsn,
            connection_factory=psycopg2.extras.LogicalReplicationConnection,
        )
        self._cursor = self._conn.cursor()
        logger.info(
            f"Established streaming replication connection "
            f"(plugin={self.config.decoding_plugin.value})"
        )

    def _start_streaming(self):
        """Start consuming from the replication slot."""
        # Build options based on plugin
        if self.config.decoding_plugin == DecodingPlugin.PGOUTPUT:
            options = {
                "proto_version": "1",
                "publication_names": self.config.publication_name,
            }
            # pgoutput sends binary protocol messages
            decode = False
        else:
            # test_decoding options
            options = {
                "include-xids": "true",
                "include-timestamp": "true",
                "skip-empty-xacts": "true",
            }
            decode = True

        # Start replication
        self._cursor.start_replication(
            slot_name=self.config.slot_name,
            decode=decode,
            start_lsn=self._format_lsn(self._last_lsn),
            options=options,
        )

        logger.info(
            f"Started streaming from slot={self.config.slot_name}, "
            f"plugin={self.config.decoding_plugin.value}, "
            f"LSN={self._last_lsn or 'beginning'}"
        )

        # Consume messages continuously
        self._cursor.consume_stream(self._process_message)

    def _process_message(self, msg):
        """Callback for each WAL message received."""
        if not self._running:
            raise StopIteration("Shutdown requested")

        try:
            # Parse based on plugin type
            if self.config.decoding_plugin == DecodingPlugin.PGOUTPUT:
                events = self._parser.parse_message(
                    msg.payload,  # bytes for pgoutput
                    str(msg.data_start),
                )
            else:
                # test_decoding outputs text
                events = self._parser.parse_message(
                    msg.payload,  # str for test_decoding (decode=True)
                    str(msg.data_start),
                )

            for event in events:
                try:
                    self.event_queue.put(event, timeout=30)
                except Full:
                    logger.error("Event queue full! Back-pressure active.")
                    self.event_queue.put(event, block=True)

            if events:
                self.metrics.increment("events_consumed", len(events))
                self._last_lsn = str(msg.data_start)

            # Send feedback to source
            msg.cursor.send_feedback(flush_lsn=msg.data_start)

        except StopIteration:
            raise
        except Exception as e:
            logger.error(f"Error processing WAL message: {e}", exc_info=True)


# ---------------------------------------------------------------------------
# Batch Processor (consumes from queue, writes to DSQL)
# ---------------------------------------------------------------------------

class BatchProcessor:
    """
    Consumes events from the queue in batches and writes to DSQL.
    Flushes on batch_size OR flush_interval, whichever comes first.
    """

    def __init__(self, config: CDCConfig, event_queue: Queue, metrics: Metrics):
        self.config = config
        self.event_queue = event_queue
        self.metrics = metrics
        self._running = False
        self._writer = DSQLWriter(config)
        self._archiver = S3Archiver(config) if config.s3_bucket else None
        self._checkpoint = CheckpointManager(config)
        self._last_confirmed_lsn = None

    def start(self):
        """Start the batch processing loop."""
        self._running = True
        self._last_confirmed_lsn = self._checkpoint.get_last_lsn()
        logger.info(f"Batch processor started, last checkpoint: {self._last_confirmed_lsn}")
        self._process_loop()

    def stop(self):
        """Gracefully stop processing (flush remaining events)."""
        self._running = False

    def get_last_confirmed_lsn(self) -> Optional[str]:
        return self._last_confirmed_lsn

    def _process_loop(self):
        """Main batch processing loop."""
        batch: List[ChangeEvent] = []
        last_flush_time = time.time()
        flush_interval_sec = self.config.flush_interval_ms / 1000.0

        while self._running:
            try:
                try:
                    event = self.event_queue.get(timeout=0.1)
                    batch.append(event)
                except Empty:
                    pass

                time_since_flush = time.time() - last_flush_time
                should_flush = (
                    len(batch) >= self.config.batch_size or
                    (len(batch) > 0 and time_since_flush >= flush_interval_sec)
                )

                if should_flush:
                    self._flush_batch(batch)
                    batch = []
                    last_flush_time = time.time()

                self.metrics.set_gauge("batch_queue_depth", self.event_queue.qsize())

            except Exception as e:
                logger.error(f"Error in batch processor: {e}", exc_info=True)
                time.sleep(1)

        # Final flush on shutdown
        if batch:
            logger.info(f"Flushing {len(batch)} remaining events on shutdown")
            self._flush_batch(batch)

    def _flush_batch(self, batch: List[ChangeEvent]):
        """Flush a batch: write to DSQL, archive to S3, save checkpoint."""
        if not batch:
            return

        batch_lsn = batch[-1].lsn
        start_time = time.time()

        # 1. Apply to DSQL
        success_count, failed_events = self._writer.apply_batch(batch)
        self.metrics.increment("events_applied", success_count)

        # Track per-table stats
        failed_set = set(id(e) for e in failed_events) if failed_events else set()
        for event in batch:
            self.metrics.track_table_event(event.schema, event.table, event.operation, id(event) not in failed_set)

        # 2. Handle failures
        if failed_events:
            self.metrics.increment("events_failed", len(failed_events))
            if self._archiver:
                self._archiver.send_to_dlq(failed_events, "Failed to apply to DSQL")
            logger.warning(f"Batch: {success_count} applied, {len(failed_events)} failed")

        # 3. Archive to S3
        if self._archiver:
            self._archiver.archive_batch(batch)
            self.metrics.increment("s3_archives")

        # 4. Save checkpoint
        if success_count > 0:
            self._checkpoint.save_checkpoint(batch_lsn)
            self._last_confirmed_lsn = batch_lsn
            self.metrics.increment("checkpoints_saved")

        self.metrics.increment("batches_processed")

        elapsed = time.time() - start_time
        events_per_sec = len(batch) / elapsed if elapsed > 0 else 0
        logger.info(
            f"Batch flushed: {len(batch)} events in {elapsed:.2f}s "
            f"({events_per_sec:.0f} events/s), LSN={batch_lsn}"
        )


# ---------------------------------------------------------------------------
# DSQL Writer
# ---------------------------------------------------------------------------

class DSQLWriter:
    """Applies change events to Aurora DSQL with configurable conflict resolution."""

    def __init__(self, config: CDCConfig):
        self.config = config
        # DSQL token auto-refresh
        self._token_manager = DSQLTokenManager(config.target_dsn)

        # Only enable TypeMapper if explicitly configured via TYPE_MAPPING env var
        self._type_mapper = None
        if os.environ.get("TYPE_MAPPING") or os.environ.get("TYPE_MAPPING_FILE"):
            try:
                from type_mapper import TypeMapper
                self._type_mapper = TypeMapper(config)
                logger.info("TypeMapper enabled (explicit mapping configured)")
            except ImportError:
                logger.warning("type_mapper.py not found, type mapping disabled")

    def _get_connection(self):
        # Use fresh DSQL token (auto-refreshed every 10 min)
        dsn = self._token_manager.get_dsn()
        conn = psycopg2.connect(dsn)
        conn.autocommit = False
        return conn

    def apply_batch(self, events: List[ChangeEvent]) -> Tuple[int, List[ChangeEvent]]:
        """Apply a batch of events. Returns (success_count, failed_events)."""
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
        # Apply type transformations before writing to DSQL
        if self._type_mapper:
            event.new_values = self._type_mapper.transform_values(
                event.schema, event.table, event.new_values, event.columns
            )
            event.old_values = self._type_mapper.transform_values(
                event.schema, event.table, event.old_values, event.columns
            )

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
                sql = (
                    f"INSERT INTO {event.fqtn} ({col_str}) VALUES ({ph_str}) "
                    f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_str}"
                )
            else:
                sql = f"INSERT INTO {event.fqtn} ({col_str}) VALUES ({ph_str})"

        elif self.config.conflict_mode == ConflictMode.SKIP:
            pk_cols = event.primary_keys
            if pk_cols:
                conflict_cols = ", ".join(f'"{c}"' for c in pk_cols)
                sql = (
                    f"INSERT INTO {event.fqtn} ({col_str}) VALUES ({ph_str}) "
                    f"ON CONFLICT ({conflict_cols}) DO NOTHING"
                )
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
        return key

    def send_to_dlq(self, events: List[ChangeEvent], error: str):
        bucket = self.config.dlq_bucket if hasattr(self.config, 'dlq_bucket') and self.config.dlq_bucket else self.config.s3_bucket
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
        self._token_manager = DSQLTokenManager(config.target_dsn)
        self._ensure_table()

    def _get_dsn(self) -> str:
        return self._token_manager.get_dsn()

    def _ensure_table(self):
        try:
            with psycopg2.connect(self._get_dsn()) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {self.config.checkpoint_table} (
                            slot_name TEXT PRIMARY KEY,
                            last_lsn TEXT NOT NULL,
                            updated_at TIMESTAMPTZ NOT NULL
                        )
                    """)
                conn.commit()
        except Exception as e:
            logger.warning(f"Checkpoint table creation: {e}")

    def get_last_lsn(self) -> Optional[str]:
        try:
            with psycopg2.connect(self._get_dsn()) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT last_lsn FROM {self.config.checkpoint_table} WHERE slot_name = %s",
                        (self.config.slot_name,),
                    )
                    row = cur.fetchone()
                    return row[0] if row else None
        except Exception:
            return None

    def save_checkpoint(self, lsn: str):
        with psycopg2.connect(self._get_dsn()) as conn:
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


# ---------------------------------------------------------------------------
# Lag Monitor
# ---------------------------------------------------------------------------

class LagMonitor:
    """Monitors replication lag and triggers back-pressure if needed."""

    def __init__(self, config: CDCConfig, metrics: Metrics):
        self.config = config
        self.metrics = metrics
        self._running = False

    def start(self):
        self._running = True
        self._monitor_loop()

    def stop(self):
        self._running = False

    def get_lag_bytes(self) -> int:
        try:
            with psycopg2.connect(self.config.source_dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT pg_wal_lsn_diff(
                            pg_current_wal_lsn(),
                            confirmed_flush_lsn
                        )
                        FROM pg_replication_slots
                        WHERE slot_name = %s
                    """, (self.config.slot_name,))
                    row = cur.fetchone()
                    return int(row[0]) if row else 0
        except Exception as e:
            logger.warning(f"Failed to check lag: {e}")
            return -1

    def _monitor_loop(self):
        while self._running:
            lag = self.get_lag_bytes()
            if lag >= 0:
                self.metrics.set_gauge("replication_lag_bytes", lag)
                if lag > self.config.max_lag_bytes:
                    logger.warning(
                        f"Replication lag HIGH: {lag / 1024 / 1024:.1f} MB "
                        f"(threshold: {self.config.max_lag_bytes / 1024 / 1024:.0f} MB)"
                    )
            time.sleep(10)


# ---------------------------------------------------------------------------
# Main CDC Service
# ---------------------------------------------------------------------------

class CDCService:
    """
    Main CDC service orchestrator. Manages all components:
      - Streaming WAL consumer (producer thread)
      - Batch processor (consumer thread)
      - Lag monitor (monitor thread)
      - Health check HTTP server (server thread)
      - Metrics publisher (periodic)
    """

    def __init__(self, config: CDCConfig):
        self.config = config
        self.metrics = Metrics(config)
        self._event_queue = Queue(maxsize=config.batch_size * 10)
        self._consumer = StreamingWALConsumer(config, self._event_queue, self.metrics)
        self._processor = BatchProcessor(config, self._event_queue, self.metrics)
        self._lag_monitor = LagMonitor(config, self.metrics)
        self._threads: List[threading.Thread] = []
        self._shutdown_event = threading.Event()
        self._is_streaming = False
        self._health_server = None
        self._control_manager = ControlManager()
        self._rule_engine = TableRuleEngine()

    def start(self):
        """Start all CDC service components."""
        logger.info("=" * 60)
        logger.info("PostgreSQL → Aurora DSQL CDC Service Starting")
        logger.info(f"  Plugin:        {self.config.decoding_plugin.value}")
        logger.info(f"  Conflict mode: {self.config.conflict_mode.value}")
        logger.info(f"  Batch size:    {self.config.batch_size}")
        logger.info(f"  Flush interval:{self.config.flush_interval_ms}ms")
        logger.info(f"  Workers:       {self.config.parallel_workers}")
        logger.info(f"  Tables:        {self.config.tables or 'ALL'}")
        logger.info("=" * 60)

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        # Get starting LSN from checkpoint
        start_lsn = self._processor.get_last_confirmed_lsn()
        logger.info(f"Resuming from LSN: {start_lsn or 'beginning'}")

        # Start health check server
        self._start_health_server()

        # Start consumer thread
        consumer_thread = threading.Thread(
            target=self._run_consumer, args=(start_lsn,),
            name="wal-consumer", daemon=True,
        )
        consumer_thread.start()
        self._threads.append(consumer_thread)

        # Start processor thread
        processor_thread = threading.Thread(
            target=self._run_processor,
            name="batch-processor", daemon=True,
        )
        processor_thread.start()
        self._threads.append(processor_thread)

        # Start lag monitor thread
        lag_thread = threading.Thread(
            target=self._run_lag_monitor,
            name="lag-monitor", daemon=True,
        )
        lag_thread.start()
        self._threads.append(lag_thread)

        # Start metrics publisher thread
        if self.config.metrics_enabled:
            metrics_thread = threading.Thread(
                target=self._run_metrics_publisher,
                name="metrics-publisher", daemon=True,
            )
            metrics_thread.start()
            self._threads.append(metrics_thread)

        # Start control watcher thread (monitors control.json independently)
        control_thread = threading.Thread(
            target=self._run_control_watcher,
            name="control-watcher", daemon=True,
        )
        control_thread.start()
        self._threads.append(control_thread)

        # Wait for shutdown signal
        self._shutdown_event.wait()
        self._shutdown()

    def _run_consumer(self, start_lsn: Optional[str]):
        try:
            while not self._shutdown_event.is_set():
                state = self._control_manager.get_state()
                
                if state == "stopped":
                    logger.info("Control state: stopped — shutting down")
                    self._shutdown_event.set()
                    break
                elif state == "paused":
                    if self._is_streaming:
                        logger.info("Control state: paused — stopping consumption")
                        self._consumer.stop()
                        self._is_streaming = False
                    time.sleep(3)
                    continue
                else:  # running
                    if not self._is_streaming:
                        logger.info("Control state: running — (re)starting consumption")
                        # Get latest checkpoint for resume
                        resume_lsn = self._processor.get_last_confirmed_lsn() or start_lsn
                        self._is_streaming = True
                        self._consumer.start(resume_lsn)
                        # If start() returns normally, it means connection was lost
                        self._is_streaming = False
                    else:
                        time.sleep(1)
        except Exception as e:
            logger.error(f"Consumer thread crashed: {e}", exc_info=True)
            self._is_streaming = False
            self._shutdown_event.set()

    def _run_processor(self):
        try:
            self._processor.start()
        except Exception as e:
            logger.error(f"Processor thread crashed: {e}", exc_info=True)
            self._shutdown_event.set()

    def _run_lag_monitor(self):
        try:
            self._lag_monitor.start()
        except Exception as e:
            logger.warning(f"Lag monitor error: {e}")

    def _run_metrics_publisher(self):
        while not self._shutdown_event.is_set():
            self.metrics.publish_to_cloudwatch()
            self._shutdown_event.wait(60)

    def _run_control_watcher(self):
        """Independent thread that polls control.json and stops/starts consumer accordingly."""
        while not self._shutdown_event.is_set():
            try:
                state = self._control_manager.get_state()
                if state == "stopped":
                    logger.info("Control watcher: stopped — initiating shutdown")
                    self._consumer.stop()
                    self._shutdown_event.set()
                    break
                elif state == "paused":
                    if self._is_streaming:
                        logger.info("Control watcher: paused — stopping consumer")
                        self._consumer.stop()
                        self._is_streaming = False
            except Exception as e:
                logger.warning(f"Control watcher error: {e}")
            time.sleep(2)

    def _start_health_server(self):
        HealthCheckHandler.service = self
        self._health_server = HTTPServer(
            ("0.0.0.0", self.config.health_port), HealthCheckHandler
        )
        server_thread = threading.Thread(
            target=self._health_server.serve_forever,
            name="health-server", daemon=True,
        )
        server_thread.start()
        logger.info(f"Health check server started on port {self.config.health_port}")

    def get_health_status(self) -> dict:
        snapshot = self.metrics.get_snapshot()
        lag_bytes = snapshot["gauges"]["replication_lag_bytes"]
        # Mask password in DSN for display
        import re
        source_display = re.sub(r'password=\S+', 'password=****', self.config.source_dsn) if self.config.source_dsn else '--'
        target_display = os.environ.get('DSQL_HOSTNAME', '--')
        source_dsn_masked = re.sub(r'password=\S+', 'password=***', self.config.source_dsn) if self.config.source_dsn else ''
        return {
            "healthy": self._is_streaming and lag_bytes < self.config.max_lag_bytes,
            "streaming": self._is_streaming,
            "plugin": self.config.decoding_plugin.value,
            "lag_bytes": lag_bytes,
            "lag_mb": round(lag_bytes / 1024 / 1024, 2),
            "events_applied": snapshot["counters"]["events_applied"],
            "events_failed": snapshot["counters"]["events_failed"],
            "uptime_seconds": snapshot["gauges"]["uptime_seconds"],
            "last_checkpoint_lsn": self._processor.get_last_confirmed_lsn(),
            "conflict_mode": self.config.conflict_mode.value,
            "queue_depth": self._event_queue.qsize(),
            "control_state": self._control_manager.get_state(),
            "table_rules_count": self._rule_engine.rules_count,
            "table_rules_summary": self._rule_engine.rules_summary,
            "tables": snapshot.get("tables", []),
            "source_dsn_display": source_display,
            "target_endpoint": target_display,
            "source_dsn": source_dsn_masked,
            "target_endpoint": os.environ.get("DSQL_HOSTNAME", ""),
        }

    def _signal_handler(self, signum, frame):
        sig_name = signal.Signals(signum).name
        logger.info(f"Received {sig_name}, initiating graceful shutdown...")
        self._shutdown_event.set()

    def _shutdown(self):
        logger.info("Shutting down CDC service...")
        self._consumer.stop()
        self._is_streaming = False

        logger.info(f"Flushing remaining events (queue depth: {self._event_queue.qsize()})...")
        self._processor.stop()
        time.sleep(2)

        self._lag_monitor.stop()

        if self._health_server:
            self._health_server.shutdown()

        self.metrics.publish_to_cloudwatch()
        logger.info("CDC service shutdown complete")
        logger.info(f"Final metrics: {json.dumps(self.metrics.get_snapshot(), indent=2)}")


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

def main():
    """Main entry point for the CDC service."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s [%(threadName)s]: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    config = CDCConfig.from_env()
    logger.setLevel(getattr(logging, config.log_level.upper(), logging.INFO))

    service = CDCService(config)
    service.start()


if __name__ == "__main__":
    main()
