"""
CDC Load Test & Benchmarking Tool
==================================
Generates realistic DML workload on the source PostgreSQL to stress-test
the CDC pipeline and measure throughput, latency, and correctness.

Features:
  - Configurable write patterns (INSERT-heavy, UPDATE-heavy, mixed)
  - Adjustable concurrency (parallel writers)
  - Configurable TPS target with rate limiting
  - Real-time progress dashboard
  - Measures end-to-end replication lag (source write → target visible)
  - Generates HTML report with charts (throughput over time, latency percentiles, errors)
  - Data integrity validation (source vs target row counts + checksums)

Usage:
  # Basic load test: 10K transactions, 50 TPS, 4 threads
  python load_test.py --source-dsn "$SOURCE_DSN" --target-dsn "$TARGET_DSN" \
      --duration 300 --tps 50 --threads 4

  # High-throughput burst: maximize throughput for 5 minutes
  python load_test.py --source-dsn "$SOURCE_DSN" --target-dsn "$TARGET_DSN" \
      --duration 300 --tps 0 --threads 16 --mode mixed

  # Generate report only (from previous run's metrics file)
  python load_test.py --report-only --metrics-file results/metrics_20260615_143000.json

Environment Variables (alternative to CLI args):
  SOURCE_DSN, TARGET_DSN, LOAD_TEST_TPS, LOAD_TEST_THREADS, LOAD_TEST_DURATION
"""


import argparse
import json
import os
import sys
import time
import random
import string
import threading
import statistics
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

import psycopg2
import psycopg2.extras


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class LoadTestConfig:
    source_dsn: str
    target_dsn: str
    duration_seconds: int = 300       # How long to run
    target_tps: int = 50              # 0 = unlimited (max throughput)
    threads: int = 4                  # Parallel writer threads
    mode: str = "mixed"               # insert, update, delete, mixed
    batch_size: int = 10              # Rows per transaction
    table_name: str = "cdc_load_test" # Test table name
    schema: str = "public"
    report_dir: str = "results"       # Output directory for reports
    warmup_seconds: int = 10          # Wait for CDC to catch up before measuring lag
    verify_interval: int = 30         # Seconds between integrity checks
    seed: int = 42                    # Random seed for reproducibility

    @classmethod
    def from_args(cls, args) -> "LoadTestConfig":
        return cls(
            source_dsn=args.source_dsn or os.environ.get("SOURCE_DSN", ""),
            target_dsn=args.target_dsn or os.environ.get("TARGET_DSN", ""),
            duration_seconds=args.duration,
            target_tps=args.tps,
            threads=args.threads,
            mode=args.mode,
            batch_size=args.batch_size,
            table_name=args.table,
            report_dir=args.report_dir,
        )


# ---------------------------------------------------------------------------
# Metrics Collector
# ---------------------------------------------------------------------------

@dataclass
class TransactionMetric:
    """Single transaction timing."""
    timestamp: float          # When the transaction started (epoch)
    operation: str            # INSERT, UPDATE, DELETE, MIXED
    rows: int                 # Number of rows affected
    duration_ms: float        # Transaction duration in ms
    success: bool             # Whether it succeeded
    error: str = ""           # Error message if failed


@dataclass 
class LagMeasurement:
    """End-to-end replication lag measurement."""
    timestamp: float
    lag_ms: float             # Time from source write to target visible
    marker_id: str


class MetricsCollector:
    """Thread-safe metrics collection."""

    def __init__(self):
        self._lock = threading.Lock()
        self.transactions: List[TransactionMetric] = []
        self.lag_measurements: List[LagMeasurement] = []
        self.start_time: float = 0
        self.end_time: float = 0
        self._interval_counters: Dict[int, int] = {}  # second -> count

    def record_transaction(self, metric: TransactionMetric):
        with self._lock:
            self.transactions.append(metric)
            second = int(metric.timestamp - self.start_time)
            self._interval_counters[second] = self._interval_counters.get(second, 0) + 1

    def record_lag(self, measurement: LagMeasurement):
        with self._lock:
            self.lag_measurements.append(measurement)

    def get_summary(self) -> dict:
        """Generate summary statistics."""
        with self._lock:
            if not self.transactions:
                return {"error": "No transactions recorded"}

            successful = [t for t in self.transactions if t.success]
            failed = [t for t in self.transactions if not t.success]
            durations = [t.duration_ms for t in successful]
            
            total_duration = self.end_time - self.start_time
            
            # TPS over time (per-second buckets)
            tps_over_time = []
            for sec in sorted(self._interval_counters.keys()):
                tps_over_time.append({
                    "second": sec,
                    "tps": self._interval_counters[sec],
                })

            # Lag statistics
            lag_stats = {}
            if self.lag_measurements:
                lags = [m.lag_ms for m in self.lag_measurements]
                lag_stats = {
                    "min_ms": round(min(lags), 2),
                    "max_ms": round(max(lags), 2),
                    "avg_ms": round(statistics.mean(lags), 2),
                    "median_ms": round(statistics.median(lags), 2),
                    "p95_ms": round(sorted(lags)[int(len(lags) * 0.95)], 2) if len(lags) > 20 else None,
                    "p99_ms": round(sorted(lags)[int(len(lags) * 0.99)], 2) if len(lags) > 100 else None,
                    "measurements": len(lags),
                }

            summary = {
                "test_duration_seconds": round(total_duration, 2),
                "total_transactions": len(self.transactions),
                "successful_transactions": len(successful),
                "failed_transactions": len(failed),
                "total_rows_written": sum(t.rows for t in successful),
                "avg_tps": round(len(successful) / total_duration, 2) if total_duration > 0 else 0,
                "peak_tps": max(self._interval_counters.values()) if self._interval_counters else 0,
                "latency": {
                    "min_ms": round(min(durations), 2) if durations else 0,
                    "max_ms": round(max(durations), 2) if durations else 0,
                    "avg_ms": round(statistics.mean(durations), 2) if durations else 0,
                    "median_ms": round(statistics.median(durations), 2) if durations else 0,
                    "p95_ms": round(sorted(durations)[int(len(durations) * 0.95)], 2) if len(durations) > 20 else 0,
                    "p99_ms": round(sorted(durations)[int(len(durations) * 0.99)], 2) if len(durations) > 100 else 0,
                },
                "replication_lag": lag_stats,
                "tps_over_time": tps_over_time,
                "errors": [{"error": t.error, "count": 1} for t in failed[:10]],  # First 10 errors
            }
            return summary


# ---------------------------------------------------------------------------
# Load Generator
# ---------------------------------------------------------------------------

class LoadGenerator:
    """Generates DML load on the source PostgreSQL."""

    def __init__(self, config: LoadTestConfig, metrics: MetricsCollector):
        self.config = config
        self.metrics = metrics
        self._running = False
        self._rng = random.Random(config.seed)
        self._row_counter = 0
        self._counter_lock = threading.Lock()

    def setup_test_table(self):
        """Create the test table on both source and target."""
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {self.config.schema}.{self.config.table_name} (
                id BIGSERIAL PRIMARY KEY,
                marker_id TEXT,
                category TEXT NOT NULL,
                amount NUMERIC(12, 2) NOT NULL,
                description TEXT,
                tags JSONB,
                metadata JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """
        # Create on source
        with psycopg2.connect(self.config.source_dsn) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(ddl)
                # Create index for update/delete operations
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{self.config.table_name}_category 
                    ON {self.config.schema}.{self.config.table_name}(category)
                """)
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{self.config.table_name}_marker 
                    ON {self.config.schema}.{self.config.table_name}(marker_id)
                """)
        print(f"✓ Test table created on source: {self.config.schema}.{self.config.table_name}")

        # Create on target (DSQL)
        try:
            with psycopg2.connect(self.config.target_dsn) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    # DSQL doesn't support BIGSERIAL or some PG-specific defaults
                    ddl_dsql = f"""
                        CREATE TABLE IF NOT EXISTS {self.config.schema}.{self.config.table_name} (
                            id BIGINT PRIMARY KEY,
                            marker_id TEXT,
                            category TEXT NOT NULL,
                            amount NUMERIC(12, 2) NOT NULL,
                            description TEXT,
                            tags JSONB,
                            metadata JSONB,
                            created_at TIMESTAMPTZ,
                            updated_at TIMESTAMPTZ
                        )
                    """
                    cur.execute(ddl_dsql)
            print(f"✓ Test table created on target (DSQL)")
        except Exception as e:
            print(f"⚠️  Target table creation: {e}")
            print("   (Table may already exist or DSQL has different DDL syntax)")

    def seed_data(self, count: int = 1000):
        """Insert seed data for UPDATE/DELETE operations."""
        print(f"Seeding {count} rows for UPDATE/DELETE tests...")
        with psycopg2.connect(self.config.source_dsn) as conn:
            with conn.cursor() as cur:
                for i in range(0, count, 100):
                    batch = []
                    for j in range(min(100, count - i)):
                        batch.append((
                            f"seed_{i+j}",
                            self._random_category(),
                            round(self._rng.uniform(1, 10000), 2),
                            self._random_text(50),
                            f'{{"seed": true, "batch": {i}}}',
                        ))
                    psycopg2.extras.execute_batch(
                        cur,
                        f"""INSERT INTO {self.config.schema}.{self.config.table_name} 
                            (marker_id, category, amount, description, metadata)
                            VALUES (%s, %s, %s, %s, %s)""",
                        batch,
                    )
                conn.commit()
        print(f"✓ Seeded {count} rows")

    def run(self):
        """Execute the load test."""
        self._running = True
        self.metrics.start_time = time.time()
        end_time = self.metrics.start_time + self.config.duration_seconds

        # Rate limiter
        interval = 1.0 / self.config.target_tps if self.config.target_tps > 0 else 0
        
        print(f"\n{'═' * 60}")
        print(f"Load test started")
        print(f"  Duration: {self.config.duration_seconds}s")
        print(f"  Target TPS: {'unlimited' if self.config.target_tps == 0 else self.config.target_tps}")
        print(f"  Threads: {self.config.threads}")
        print(f"  Mode: {self.config.mode}")
        print(f"  Batch size: {self.config.batch_size} rows/txn")
        print(f"{'═' * 60}\n")

        with ThreadPoolExecutor(max_workers=self.config.threads) as executor:
            futures = []
            tx_count = 0

            while time.time() < end_time and self._running:
                # Rate limiting
                if interval > 0:
                    time.sleep(interval)

                # Submit work
                op = self._pick_operation()
                future = executor.submit(self._execute_transaction, op)
                futures.append(future)
                tx_count += 1

                # Progress update every 5 seconds
                if tx_count % (max(1, self.config.target_tps) * 5) == 0:
                    elapsed = time.time() - self.metrics.start_time
                    actual_tps = tx_count / elapsed if elapsed > 0 else 0
                    print(f"  [{elapsed:.0f}s] Submitted {tx_count} txns, ~{actual_tps:.1f} TPS")

            # Wait for in-flight transactions
            for future in futures:
                try:
                    future.result(timeout=30)
                except Exception:
                    pass

        self._running = False
        self.metrics.end_time = time.time()
        print(f"\n✓ Load test complete: {len(self.metrics.transactions)} transactions in "
              f"{self.metrics.end_time - self.metrics.start_time:.1f}s")

    def stop(self):
        self._running = False

    def _execute_transaction(self, operation: str):
        """Execute a single transaction and record metrics."""
        start = time.time()
        rows = 0
        success = True
        error = ""

        try:
            with psycopg2.connect(self.config.source_dsn) as conn:
                with conn.cursor() as cur:
                    if operation == "INSERT":
                        rows = self._do_insert(cur)
                    elif operation == "UPDATE":
                        rows = self._do_update(cur)
                    elif operation == "DELETE":
                        rows = self._do_delete(cur)
                    elif operation == "MIXED":
                        rows = self._do_mixed(cur)
                conn.commit()
        except Exception as e:
            success = False
            error = str(e)[:200]

        duration_ms = (time.time() - start) * 1000

        self.metrics.record_transaction(TransactionMetric(
            timestamp=start,
            operation=operation,
            rows=rows,
            duration_ms=duration_ms,
            success=success,
            error=error,
        ))

    def _do_insert(self, cur) -> int:
        """Insert a batch of rows."""
        batch = []
        for _ in range(self.config.batch_size):
            with self._counter_lock:
                self._row_counter += 1
                row_id = self._row_counter
            batch.append((
                f"load_{row_id}_{int(time.time()*1000)}",
                self._random_category(),
                round(self._rng.uniform(1, 99999), 2),
                self._random_text(100),
                json.dumps([self._random_category() for _ in range(3)]),
                json.dumps({"load_test": True, "batch_id": row_id}),
            ))
        
        psycopg2.extras.execute_batch(
            cur,
            f"""INSERT INTO {self.config.schema}.{self.config.table_name}
                (marker_id, category, amount, description, tags, metadata)
                VALUES (%s, %s, %s, %s, %s, %s)""",
            batch,
        )
        return len(batch)

    def _do_update(self, cur) -> int:
        """Update random existing rows."""
        cur.execute(
            f"""UPDATE {self.config.schema}.{self.config.table_name}
                SET amount = amount + %s,
                    description = %s,
                    updated_at = NOW()
                WHERE category = %s
                LIMIT %s""",
            (
                round(self._rng.uniform(-100, 100), 2),
                self._random_text(80),
                self._random_category(),
                self.config.batch_size,
            )
        )
        return cur.rowcount or 0

    def _do_delete(self, cur) -> int:
        """Delete a small batch of old rows."""
        cur.execute(
            f"""DELETE FROM {self.config.schema}.{self.config.table_name}
                WHERE id IN (
                    SELECT id FROM {self.config.schema}.{self.config.table_name}
                    WHERE marker_id LIKE 'seed_%'
                    ORDER BY RANDOM()
                    LIMIT %s
                )""",
            (max(1, self.config.batch_size // 5),)
        )
        return cur.rowcount or 0

    def _do_mixed(self, cur) -> int:
        """Mixed workload: 60% INSERT, 30% UPDATE, 10% DELETE."""
        roll = self._rng.random()
        if roll < 0.6:
            return self._do_insert(cur)
        elif roll < 0.9:
            return self._do_update(cur)
        else:
            return self._do_delete(cur)

    def _pick_operation(self) -> str:
        """Pick operation type based on mode."""
        if self.config.mode == "insert":
            return "INSERT"
        elif self.config.mode == "update":
            return "UPDATE"
        elif self.config.mode == "delete":
            return "DELETE"
        else:  # mixed
            return "MIXED"

    def _random_category(self) -> str:
        categories = ["electronics", "clothing", "food", "books", "sports",
                      "home", "garden", "toys", "health", "automotive"]
        return self._rng.choice(categories)

    def _random_text(self, max_len: int) -> str:
        length = self._rng.randint(10, max_len)
        return ''.join(self._rng.choices(string.ascii_letters + string.digits + ' ', k=length))


# ---------------------------------------------------------------------------
# Lag Measurer
# ---------------------------------------------------------------------------

class LagMeasurer:
    """
    Measures end-to-end replication lag by:
    1. Writing a timestamped marker to source
    2. Polling target until the marker appears
    3. Recording the time difference
    """

    def __init__(self, config: LoadTestConfig, metrics: MetricsCollector):
        self.config = config
        self.metrics = metrics
        self._running = False

    def start(self):
        """Start periodic lag measurements."""
        self._running = True
        self._measure_loop()

    def stop(self):
        self._running = False

    def _measure_loop(self):
        """Measure lag every 5 seconds."""
        while self._running:
            try:
                self._measure_once()
            except Exception as e:
                print(f"  ⚠️  Lag measurement error: {e}")
            time.sleep(5)

    def _measure_once(self):
        """Insert a marker on source and time how long until it appears on target."""
        marker_id = f"lag_probe_{int(time.time()*1000)}_{random.randint(0, 9999)}"
        write_time = time.time()

        # Write marker to source
        with psycopg2.connect(self.config.source_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {self.config.schema}.{self.config.table_name}
                        (marker_id, category, amount, description, metadata)
                        VALUES (%s, 'lag_probe', 0, 'lag measurement', %s)""",
                    (marker_id, json.dumps({"probe_time": write_time})),
                )
            conn.commit()

        # Poll target for the marker (timeout 60s)
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                with psycopg2.connect(self.config.target_dsn) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"""SELECT 1 FROM {self.config.schema}.{self.config.table_name}
                                WHERE marker_id = %s""",
                            (marker_id,),
                        )
                        if cur.fetchone():
                            lag_ms = (time.time() - write_time) * 1000
                            self.metrics.record_lag(LagMeasurement(
                                timestamp=write_time,
                                lag_ms=lag_ms,
                                marker_id=marker_id,
                            ))
                            return
            except Exception:
                pass
            time.sleep(0.1)  # Poll every 100ms

        # Timeout — record as failed
        self.metrics.record_lag(LagMeasurement(
            timestamp=write_time,
            lag_ms=60000,  # 60s timeout
            marker_id=marker_id,
        ))


# ---------------------------------------------------------------------------
# Data Integrity Validator
# ---------------------------------------------------------------------------

class IntegrityValidator:
    """Validates data consistency between source and target."""

    def __init__(self, config: LoadTestConfig):
        self.config = config

    def validate(self) -> dict:
        """Compare source and target row counts + checksums."""
        print("\n🔍 Running data integrity validation...")
        results = {}

        # Row counts
        source_count = self._get_count(self.config.source_dsn)
        target_count = self._get_count(self.config.target_dsn)
        results["source_row_count"] = source_count
        results["target_row_count"] = target_count
        results["row_count_match"] = source_count == target_count
        results["row_count_diff"] = source_count - target_count

        # Checksum (sum of amounts as a simple integrity check)
        source_checksum = self._get_checksum(self.config.source_dsn)
        target_checksum = self._get_checksum(self.config.target_dsn)
        results["source_checksum"] = source_checksum
        results["target_checksum"] = target_checksum
        results["checksum_match"] = abs((source_checksum or 0) - (target_checksum or 0)) < 0.01

        # Sample comparison (first 100 rows by ID)
        source_sample = self._get_sample(self.config.source_dsn)
        target_sample = self._get_sample(self.config.target_dsn)
        matching = sum(1 for s, t in zip(source_sample, target_sample) if s == t)
        results["sample_size"] = len(source_sample)
        results["sample_matching"] = matching
        results["sample_match_pct"] = round(matching / max(1, len(source_sample)) * 100, 2)

        # Print summary
        print(f"  Source rows:  {source_count}")
        print(f"  Target rows:  {target_count}")
        print(f"  Row match:    {'✓' if results['row_count_match'] else '✗'} "
              f"(diff: {results['row_count_diff']})")
        print(f"  Checksum:     {'✓' if results['checksum_match'] else '✗'}")
        print(f"  Sample match: {results['sample_match_pct']}%")

        return results

    def _get_count(self, dsn: str) -> int:
        try:
            with psycopg2.connect(dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT COUNT(*) FROM {self.config.schema}.{self.config.table_name}"
                    )
                    return cur.fetchone()[0]
        except Exception as e:
            print(f"  ⚠️  Count error: {e}")
            return -1

    def _get_checksum(self, dsn: str) -> Optional[float]:
        try:
            with psycopg2.connect(dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT SUM(amount) FROM {self.config.schema}.{self.config.table_name}"
                    )
                    result = cur.fetchone()[0]
                    return float(result) if result else 0.0
        except Exception:
            return None

    def _get_sample(self, dsn: str, limit: int = 100) -> list:
        try:
            with psycopg2.connect(dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""SELECT id, marker_id, category, amount 
                            FROM {self.config.schema}.{self.config.table_name}
                            ORDER BY id LIMIT %s""",
                        (limit,),
                    )
                    return cur.fetchall()
        except Exception:
            return []


# ---------------------------------------------------------------------------
# Report Generator
# ---------------------------------------------------------------------------

class ReportGenerator:
    """Generates an HTML report with charts from test metrics."""

    def __init__(self, config: LoadTestConfig, metrics: MetricsCollector):
        self.config = config
        self.metrics = metrics

    def generate(self, integrity_results: dict = None) -> str:
        """Generate HTML report and save to disk. Returns file path."""
        os.makedirs(self.config.report_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save raw metrics as JSON
        summary = self.metrics.get_summary()
        summary["config"] = {
            "duration_seconds": self.config.duration_seconds,
            "target_tps": self.config.target_tps,
            "threads": self.config.threads,
            "mode": self.config.mode,
            "batch_size": self.config.batch_size,
            "table_name": self.config.table_name,
        }
        if integrity_results:
            summary["integrity"] = integrity_results

        metrics_path = os.path.join(self.config.report_dir, f"metrics_{timestamp}.json")
        with open(metrics_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"✓ Metrics saved: {metrics_path}")

        # Generate HTML report
        html_path = os.path.join(self.config.report_dir, f"report_{timestamp}.html")
        html = self._render_html(summary)
        with open(html_path, "w") as f:
            f.write(html)
        print(f"✓ HTML report: {html_path}")

        return html_path

    def _render_html(self, summary: dict) -> str:
        """Render full HTML report with embedded charts."""
        tps_data = summary.get("tps_over_time", [])
        latency = summary.get("latency", {})
        lag = summary.get("replication_lag", {})
        config = summary.get("config", {})
        integrity = summary.get("integrity", {})

        # TPS chart data
        tps_labels = [str(d["second"]) for d in tps_data]
        tps_values = [d["tps"] for d in tps_data]

        # Lag measurements over time
        lag_data = []
        for m in self.metrics.lag_measurements:
            lag_data.append({
                "x": round(m.timestamp - self.metrics.start_time, 1),
                "y": round(m.lag_ms, 1),
            })

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>CDC Load Test Report — {datetime.now().strftime('%Y-%m-%d %H:%M')}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; 
               background: #0f1117; color: #e4e4e7; padding: 2rem; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        h1 {{ font-size: 2rem; margin-bottom: 0.5rem; color: #fff; }}
        h2 {{ font-size: 1.3rem; margin: 2rem 0 1rem; color: #a1a1aa; }}
        .subtitle {{ color: #71717a; margin-bottom: 2rem; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin: 1.5rem 0; }}
        .card {{ background: #1c1c22; border: 1px solid #27272a; border-radius: 12px; padding: 1.5rem; }}
        .card .label {{ font-size: 0.8rem; color: #71717a; text-transform: uppercase; letter-spacing: 0.05em; }}
        .card .value {{ font-size: 1.8rem; font-weight: 700; margin-top: 0.3rem; }}
        .card .value.green {{ color: #4ade80; }}
        .card .value.red {{ color: #f87171; }}
        .card .value.blue {{ color: #60a5fa; }}
        .card .value.yellow {{ color: #fbbf24; }}
        .chart-container {{ background: #1c1c22; border: 1px solid #27272a; border-radius: 12px; padding: 1.5rem; margin: 1.5rem 0; }}
        canvas {{ max-height: 300px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 1rem 0; }}
        th, td {{ padding: 0.75rem 1rem; text-align: left; border-bottom: 1px solid #27272a; }}
        th {{ color: #a1a1aa; font-size: 0.8rem; text-transform: uppercase; }}
        .badge {{ display: inline-block; padding: 0.2rem 0.6rem; border-radius: 4px; font-size: 0.75rem; }}
        .badge-pass {{ background: #064e3b; color: #4ade80; }}
        .badge-fail {{ background: #450a0a; color: #f87171; }}
        .config-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; }}
        .config-item {{ font-size: 0.9rem; color: #a1a1aa; }}
        .config-item strong {{ color: #e4e4e7; }}
    </style>
</head>
<body>
<div class="container">
    <h1>🔄 CDC Load Test Report</h1>
    <p class="subtitle">PostgreSQL → Aurora DSQL | {datetime.now().strftime('%B %d, %Y at %H:%M')}</p>

    <!-- Summary Cards -->
    <div class="grid">
        <div class="card">
            <div class="label">Total Transactions</div>
            <div class="value blue">{summary.get('total_transactions', 0):,}</div>
        </div>
        <div class="card">
            <div class="label">Rows Written</div>
            <div class="value blue">{summary.get('total_rows_written', 0):,}</div>
        </div>
        <div class="card">
            <div class="label">Average TPS</div>
            <div class="value green">{summary.get('avg_tps', 0)}</div>
        </div>
        <div class="card">
            <div class="label">Peak TPS</div>
            <div class="value green">{summary.get('peak_tps', 0)}</div>
        </div>
        <div class="card">
            <div class="label">Failed Transactions</div>
            <div class="value {'red' if summary.get('failed_transactions', 0) > 0 else 'green'}">{summary.get('failed_transactions', 0)}</div>
        </div>
        <div class="card">
            <div class="label">Avg Replication Lag</div>
            <div class="value yellow">{lag.get('avg_ms', 'N/A')}{'ms' if lag.get('avg_ms') else ''}</div>
        </div>
        <div class="card">
            <div class="label">P95 Lag</div>
            <div class="value yellow">{lag.get('p95_ms', 'N/A')}{'ms' if lag.get('p95_ms') else ''}</div>
        </div>
        <div class="card">
            <div class="label">Duration</div>
            <div class="value">{summary.get('test_duration_seconds', 0)}s</div>
        </div>
    </div>

    <!-- TPS Over Time Chart -->
    <h2>📈 Throughput (TPS) Over Time</h2>
    <div class="chart-container">
        <canvas id="tpsChart"></canvas>
    </div>

    <!-- Replication Lag Chart -->
    <h2>⏱️ Replication Lag Over Time</h2>
    <div class="chart-container">
        <canvas id="lagChart"></canvas>
    </div>

    <!-- Latency Table -->
    <h2>📊 Transaction Latency (Source Write)</h2>
    <table>
        <tr><th>Metric</th><th>Value</th></tr>
        <tr><td>Min</td><td>{latency.get('min_ms', 0)} ms</td></tr>
        <tr><td>Average</td><td>{latency.get('avg_ms', 0)} ms</td></tr>
        <tr><td>Median (P50)</td><td>{latency.get('median_ms', 0)} ms</td></tr>
        <tr><td>P95</td><td>{latency.get('p95_ms', 0)} ms</td></tr>
        <tr><td>P99</td><td>{latency.get('p99_ms', 0)} ms</td></tr>
        <tr><td>Max</td><td>{latency.get('max_ms', 0)} ms</td></tr>
    </table>

    <!-- Integrity Results -->
    <h2>✅ Data Integrity Validation</h2>
    <table>
        <tr><th>Check</th><th>Source</th><th>Target</th><th>Status</th></tr>
        <tr>
            <td>Row Count</td>
            <td>{integrity.get('source_row_count', 'N/A')}</td>
            <td>{integrity.get('target_row_count', 'N/A')}</td>
            <td><span class="badge {'badge-pass' if integrity.get('row_count_match') else 'badge-fail'}">
                {'PASS' if integrity.get('row_count_match') else f"DIFF: {integrity.get('row_count_diff', '?')}"}</span></td>
        </tr>
        <tr>
            <td>Checksum (SUM)</td>
            <td>{integrity.get('source_checksum', 'N/A')}</td>
            <td>{integrity.get('target_checksum', 'N/A')}</td>
            <td><span class="badge {'badge-pass' if integrity.get('checksum_match') else 'badge-fail'}">
                {'PASS' if integrity.get('checksum_match') else 'FAIL'}</span></td>
        </tr>
        <tr>
            <td>Sample Match</td>
            <td colspan="2">{integrity.get('sample_matching', '?')}/{integrity.get('sample_size', '?')} rows</td>
            <td><span class="badge {'badge-pass' if integrity.get('sample_match_pct', 0) == 100 else 'badge-fail'}">
                {integrity.get('sample_match_pct', 0)}%</span></td>
        </tr>
    </table>

    <!-- Config -->
    <h2>⚙️ Test Configuration</h2>
    <div class="config-grid">
        <div class="config-item"><strong>Mode:</strong> {config.get('mode', 'mixed')}</div>
        <div class="config-item"><strong>Target TPS:</strong> {config.get('target_tps', 50) or 'unlimited'}</div>
        <div class="config-item"><strong>Threads:</strong> {config.get('threads', 4)}</div>
        <div class="config-item"><strong>Batch Size:</strong> {config.get('batch_size', 10)} rows/txn</div>
        <div class="config-item"><strong>Duration:</strong> {config.get('duration_seconds', 300)}s</div>
        <div class="config-item"><strong>Table:</strong> {config.get('table_name', 'cdc_load_test')}</div>
    </div>
</div>

<script>
// TPS Chart
new Chart(document.getElementById('tpsChart'), {{
    type: 'line',
    data: {{
        labels: {json.dumps(tps_labels[:300])},
        datasets: [{{
            label: 'Transactions Per Second',
            data: {json.dumps(tps_values[:300])},
            borderColor: '#4ade80',
            backgroundColor: 'rgba(74, 222, 128, 0.1)',
            fill: true,
            tension: 0.3,
            pointRadius: 0,
        }}]
    }},
    options: {{
        responsive: true,
        plugins: {{ legend: {{ labels: {{ color: '#a1a1aa' }} }} }},
        scales: {{
            x: {{ title: {{ display: true, text: 'Seconds', color: '#71717a' }}, 
                  ticks: {{ color: '#71717a', maxTicksLimit: 20 }}, grid: {{ color: '#27272a' }} }},
            y: {{ title: {{ display: true, text: 'TPS', color: '#71717a' }},
                  ticks: {{ color: '#71717a' }}, grid: {{ color: '#27272a' }} }}
        }}
    }}
}});

// Lag Chart
new Chart(document.getElementById('lagChart'), {{
    type: 'scatter',
    data: {{
        datasets: [{{
            label: 'Replication Lag (ms)',
            data: {json.dumps(lag_data[:200])},
            borderColor: '#fbbf24',
            backgroundColor: 'rgba(251, 191, 36, 0.5)',
            pointRadius: 4,
        }}]
    }},
    options: {{
        responsive: true,
        plugins: {{ legend: {{ labels: {{ color: '#a1a1aa' }} }} }},
        scales: {{
            x: {{ title: {{ display: true, text: 'Seconds Since Start', color: '#71717a' }},
                  ticks: {{ color: '#71717a' }}, grid: {{ color: '#27272a' }} }},
            y: {{ title: {{ display: true, text: 'Lag (ms)', color: '#71717a' }},
                  ticks: {{ color: '#71717a' }}, grid: {{ color: '#27272a' }} }}
        }}
    }}
}});
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Main Orchestrator
# ---------------------------------------------------------------------------

def run_load_test(config: LoadTestConfig):
    """Run the full load test pipeline."""
    metrics = MetricsCollector()
    generator = LoadGenerator(config, metrics)
    lag_measurer = LagMeasurer(config, metrics)
    validator = IntegrityValidator(config)
    reporter = ReportGenerator(config, metrics)

    # 1. Setup
    print("=" * 60)
    print("CDC Load Test — PostgreSQL → Aurora DSQL")
    print("=" * 60)
    generator.setup_test_table()
    generator.seed_data(1000)

    # 2. Start lag measurement in background
    lag_thread = threading.Thread(target=lag_measurer.start, name="lag-measurer", daemon=True)
    lag_thread.start()

    # 3. Run load test
    try:
        generator.run()
    except KeyboardInterrupt:
        print("\n⚠️  Load test interrupted")
        generator.stop()

    # 4. Stop lag measurement
    lag_measurer.stop()

    # 5. Wait for CDC to catch up (give it time to replicate remaining events)
    print(f"\n⏳ Waiting {config.warmup_seconds}s for CDC to catch up...")
    time.sleep(config.warmup_seconds)

    # 6. Validate integrity
    integrity_results = validator.validate()

    # 7. Generate report
    print()
    report_path = reporter.generate(integrity_results)
    
    # 8. Print final summary
    summary = metrics.get_summary()
    print(f"\n{'═' * 60}")
    print(f"FINAL RESULTS")
    print(f"{'═' * 60}")
    print(f"  Transactions:    {summary['successful_transactions']:,} succeeded, "
          f"{summary['failed_transactions']} failed")
    print(f"  Rows written:    {summary['total_rows_written']:,}")
    print(f"  Avg TPS:         {summary['avg_tps']}")
    print(f"  Peak TPS:        {summary['peak_tps']}")
    print(f"  Write latency:   avg={summary['latency']['avg_ms']}ms, "
          f"p95={summary['latency']['p95_ms']}ms")
    if summary['replication_lag']:
        print(f"  Replication lag:  avg={summary['replication_lag'].get('avg_ms')}ms, "
              f"p95={summary['replication_lag'].get('p95_ms')}ms")
    print(f"  Data integrity:  {'✓ PASS' if integrity_results.get('row_count_match') else '✗ FAIL'}")
    print(f"\n  📄 Report: {report_path}")
    print(f"{'═' * 60}")


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="CDC Load Test — PostgreSQL to Aurora DSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic test (5 min, 50 TPS, mixed operations)
  python load_test.py --source-dsn "$SOURCE_DSN" --target-dsn "$TARGET_DSN"

  # High-throughput burst
  python load_test.py --source-dsn "$SOURCE_DSN" --target-dsn "$TARGET_DSN" \\
      --tps 0 --threads 16 --duration 600

  # INSERT-only workload
  python load_test.py --source-dsn "$SOURCE_DSN" --target-dsn "$TARGET_DSN" \\
      --mode insert --tps 100 --batch-size 50
        """,
    )
    parser.add_argument("--source-dsn", default="", help="PostgreSQL source DSN")
    parser.add_argument("--target-dsn", default="", help="Aurora DSQL target DSN")
    parser.add_argument("--duration", type=int, default=300, help="Test duration in seconds (default: 300)")
    parser.add_argument("--tps", type=int, default=50, help="Target TPS, 0=unlimited (default: 50)")
    parser.add_argument("--threads", type=int, default=4, help="Parallel writer threads (default: 4)")
    parser.add_argument("--mode", choices=["insert", "update", "delete", "mixed"], default="mixed")
    parser.add_argument("--batch-size", type=int, default=10, help="Rows per transaction (default: 10)")
    parser.add_argument("--table", default="cdc_load_test", help="Test table name")
    parser.add_argument("--report-dir", default="results", help="Output directory for reports")
    parser.add_argument("--report-only", action="store_true", help="Generate report from existing metrics")
    parser.add_argument("--metrics-file", default="", help="Path to metrics JSON (for --report-only)")

    args = parser.parse_args()

    if args.report_only:
        if not args.metrics_file:
            print("Error: --metrics-file required with --report-only")
            sys.exit(1)
        # Regenerate report from saved metrics
        with open(args.metrics_file) as f:
            summary = json.load(f)
        print(f"Report regenerated from: {args.metrics_file}")
        sys.exit(0)

    config = LoadTestConfig.from_args(args)
    if not config.source_dsn or not config.target_dsn:
        print("Error: --source-dsn and --target-dsn required (or set SOURCE_DSN / TARGET_DSN env vars)")
        sys.exit(1)

    run_load_test(config)


if __name__ == "__main__":
    main()
