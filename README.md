# PostgreSQL to Amazon Aurora DSQL — Near Real-time CDC Replicator

A production-ready Change Data Capture (CDC) tool that replicates data from PostgreSQL 14+ to Amazon Aurora DSQL in near real-time using logical replication.

---

## Architecture

```
┌─────────────────┐         ┌──────────────────────────────────────────┐
│  Source          │         │  AWS Account                             │
│  PostgreSQL 14+  │◄────────┤                                          │
│  (RDS/Self-mgd)  │  WAL    │  EC2 Instance (t3.medium)                │
└─────────────────┘  Stream  │  ┌────────────────────────────────┐      │
                             │  │  cdc_service.py (systemd)       │      │
                             │  │  - StreamingWALConsumer          │      │
                             │  │  - BatchProcessor               │      │
                             │  │  - DSQLWriter (parallel)         │      │
                             │  │  - Health Server (:8080)         │      │
                             │  └────────────────────────────────┘      │
                             │              │                            │
                             │              ▼                            │
┌─────────────────┐         │  ┌────────────────────────────────┐      │
│  Target          │◄────────┤  │  Aurora DSQL (IAM Auth)         │      │
│  Aurora DSQL     │  Upsert │  └────────────────────────────────┘      │
└─────────────────┘         │                                          │
                             │  ┌────────────────────────────────┐      │
                             │  │  CloudFront + S3 (Dashboard)    │      │
                             │  │  API Gateway + Lambda (Proxy)   │      │
                             │  └────────────────────────────────┘      │
                             └──────────────────────────────────────────┘
```

---

## Features

- **Near real-time replication** — streams WAL changes with sub-second latency
- **Zero data loss** — WAL is only confirmed after successful DSQL write
- **Configurable conflict resolution** — upsert, skip, fail, or last_write_wins
- **DMS-style table mapping** — include/exclude rules with `%` wildcards
- **Secrets Manager integration** — source credentials via ARN (no plaintext passwords)
- **IAM token authentication** — auto-refreshing DSQL tokens (every 10 min)
- **Built-in load testing** — fully isolated sample tables with dedicated replication slot
- **Web dashboard** — step-by-step configuration, monitoring, and control
- **One-click CloudFormation deployment** — single template deploys everything
- **Pre-flight connectivity check** — verifies source & target before starting

---

## Prerequisites

### AWS Resources (must exist before deployment)

| Resource | Description |
|----------|-------------|
| **VPC** | With at least one public subnet (EC2 needs internet for git clone) |
| **Security Group** | Allow inbound on port 8080 (self-referencing), outbound all |
| **RDS PostgreSQL 14+** | Source database in the same VPC |
| **Aurora DSQL Cluster** | Target cluster (any region) |
| **Secrets Manager Secret** | Source DB credentials in JSON format |
| **GitHub Repository** | This repo (public, or with access configured) |

### Source PostgreSQL Configuration

```sql
-- Required PostgreSQL parameters (RDS Parameter Group):
-- wal_level = logical
-- max_replication_slots >= 2
-- max_wal_senders >= 2

-- Create replication slot:
SELECT pg_create_logical_replication_slot('dsql_cdc_slot', 'test_decoding');

-- Set REPLICA IDENTITY on tables you want to replicate:
ALTER TABLE your_table REPLICA IDENTITY FULL;
```

### Secrets Manager Secret Format

```json
{
  "username": "postgres",
  "password": "your-password",
  "host": "your-rds-endpoint.region.rds.amazonaws.com",
  "port": 5432,
  "dbname": "postgres"
}
```

### Target Aurora DSQL

- Tables must be pre-created on DSQL with matching schema
- DSQL does NOT support: `SERIAL`, `FOREIGN KEY`, `ARRAY` types, multiple DDL in one transaction
- Use `INT` with application-generated IDs instead of `SERIAL`
- Use `JSONB` instead of array types

---

## Deployment (One-Click CloudFormation)

### Step 1: Deploy the Stack

1. Open **CloudFormation** → Create Stack → Upload template
2. Upload `infra/master-stack.yaml`
3. Fill in parameters:

| Parameter | Description | Example |
|-----------|-------------|---------|
| VpcId | VPC where RDS resides | vpc-0636bd5b5bf187e9b |
| SubnetIds | Public subnet(s) | subnet-abc123 |
| SecurityGroupId | SG with port 8080 self-referencing | sg-xyz789 |
| GitHubRepoOwner | GitHub username or org | mathurravi23 |
| GitHubRepoName | Repository name | pg-dsql-cdc |
| GitHubBranch | Branch to deploy | main |
| SlotName | Logical replication slot name | dsql_cdc_slot |
| ConflictMode | Conflict handling strategy | upsert |
| BatchSize | Events per batch | 1000 |

4. Check **"I acknowledge that AWS CloudFormation might create IAM resources"**
5. Click **Create Stack** (~10 min to complete)

### Step 2: Access the Dashboard

After stack creation:
1. Go to **CloudFormation → Outputs**
2. Find `DashboardURL` — open in browser
3. The dashboard has 4 steps to follow in order

---

## Post-Deployment Setup (Dashboard)

### Step 1: Configuration

1. **Source PostgreSQL** — Enter your Secrets Manager ARN:
   ```
   arn:aws:secretsmanager:us-east-1:123456789:secret=[REDACTED_PASSWORD]
   ```
2. **Target DSQL** — Enter the DSQL cluster endpoint:
   ```
   your-cluster-id.dsql.us-east-1.on.aws
   ```
3. Click **Test Connectivity** to verify both connections
4. Click **Save DSN**
5. Set **Batch Size**, **Conflict Mode**, **Slot Name** → **Save Configuration**

### Step 2: Test with Sample Data (Optional)

1. Click **▶ Start Load Test**
2. The load test creates its own `sample_*` tables and dedicated slot
3. Monitor the "Sample Table Replication Status" table
4. Verify data appears on target DSQL
5. Test slot is automatically cleaned up after completion

### Step 3: Table Selection for Replication

Define which tables to replicate using DMS-style JSON rules:

```json
{
  "rules": [
    {
      "rule-type": "selection",
      "rule-id": "1",
      "rule-name": "include-all-public",
      "object-locator": {"schema-name": "public", "table-name": "%"},
      "rule-action": "include"
    },
    {
      "rule-type": "selection",
      "rule-id": "2",
      "rule-name": "exclude-temp-tables",
      "object-locator": {"schema-name": "public", "table-name": "tmp_%"},
      "rule-action": "exclude"
    }
  ]
}
```

Click **Apply Rules**.

### Step 4: Replication Task

1. Click **▶ Start** to begin replication
2. Monitor the table status for events applied/failed
3. Click **⏹ Stop** to pause (WAL is held by PostgreSQL, no data loss)

---

## Configuration Reference

### Environment Variables (`.env`)

| Variable | Required | Description |
|----------|----------|-------------|
| SOURCE_DSN | Yes | Secrets Manager ARN or raw PostgreSQL DSN |
| TARGET_DSN | Yes | `host=<endpoint> port=5432 dbname=postgres user=admin sslmode=require` |
| DSQL_HOSTNAME | Yes | Aurora DSQL cluster endpoint |
| DSQL_REGION | Yes | AWS region of DSQL cluster |
| SLOT_NAME | Yes | PostgreSQL logical replication slot name |
| CONFLICT_MODE | No | `upsert` (default), `skip`, `fail`, `last_write_wins` |
| BATCH_SIZE | No | Events per batch (default: 1000) |
| PARALLEL_WORKERS | No | Concurrent DSQL writers (default: 4) |
| DECODING_PLUGIN | No | `test_decoding` (default) or `pgoutput` |
| FLUSH_INTERVAL_MS | No | Max time before batch flush (default: 500) |
| HEALTH_PORT | No | Health server port (default: 8080) |
| METRICS_ENABLED | No | CloudWatch metrics (default: true) |

### Conflict Resolution Modes

| Mode | Behavior |
|------|----------|
| **upsert** | INSERT or UPDATE on conflict (default, recommended) |
| **skip** | Skip conflicting rows silently |
| **fail** | Error on conflict (batch fails) |
| **last_write_wins** | Always overwrite with latest value |

---

## Monitoring

### CloudWatch Dashboard

A CloudWatch dashboard is auto-created with:
- Events Applied/Failed per minute
- Replication Lag (bytes)
- Batch Queue Depth
- Service Health

### Health Endpoint

```bash
curl http://<ec2-private-ip>:8080/health
```

Returns:
```json
{
  "healthy": true,
  "streaming": true,
  "events_applied": 73015,
  "events_failed": 0,
  "lag_bytes": 0,
  "control_state": "running",
  "slot_name": "dsql_cdc_slot",
  "batch_size": 1000,
  "conflict_mode": "upsert"
}
```

### CloudWatch Logs

| Log Group | Content |
|-----------|---------|
| `/ec2/<stack-name>-cdc` | CDC service logs (replication events, errors) |
| `/ec2/<stack-name>-cdc` (loadtest stream) | Load test output |

---

## Data Loss Prevention

This tool implements **zero data loss guarantees**:

1. **WAL feedback only after confirmed write** — PostgreSQL WAL is NOT confirmed until data is successfully written to DSQL
2. **Safe checkpoint advancement** — Checkpoint LSN never advances past failed events
3. **Pre-flight connectivity check** — Verifies both source and target are reachable before starting
4. **WAL retention on pause/stop** — PostgreSQL holds WAL segments until replication resumes

### Recovery Scenarios

| Scenario | Behavior |
|----------|----------|
| Service crash | Restarts from last checkpoint, replays unconfirmed events |
| DSQL unavailable | Events queue up, retried when connection restored |
| Network partition | PostgreSQL holds WAL, resumes from last confirmed LSN |
| Token expiration | Auto-refreshes every 10 min (token TTL: 15 min) |

---

## Limitations

| Limitation | Details |
|------------|---------|
| **DDL not replicated** | Schema changes must be applied manually on both source and target |
| **DSQL type restrictions** | No `SERIAL`, `ARRAY`, `FOREIGN KEY`, multi-DDL transactions |
| **Single source** | One PostgreSQL source per deployment |
| **No initial full load** | Only captures changes after slot creation (use pg_dump for initial load) |
| **test_decoding only** | `pgoutput` support is implemented but less tested |
| **No schema evolution** | Adding/removing columns requires manual intervention |
| **Table must pre-exist on target** | Target tables must be created manually before replication |
| **REPLICA IDENTITY required** | Source tables need `REPLICA IDENTITY FULL` for UPDATE/DELETE replication |

---

## Known Issues

| Issue | Workaround |
|-------|-----------|
| CloudWatch metrics "disabled" warning on startup | Non-fatal — CloudWatch `PutMetricData` needs correct namespace; metrics still work |
| Checkpoint table creation warning (invalid DSN) | Non-fatal at startup if TARGET_DSN format uses just hostname; resolves once replication starts |
| Load test output buffering | Logs appear all at once after test completes (Python stdout buffering through tee) |
| CloudFront cache on frontend updates | Hard-refresh (Ctrl+Shift+R) or invalidate: `aws cloudfront create-invalidation --distribution-id <ID> --paths "/*"` |
| Stack deletion takes 15-20 min | CloudFront distribution deletion is slow (global edge propagation); Lambda VPC ENI cleanup adds time |

---

## Manual Operations

### SSH to EC2

```bash
# Via SSM Session Manager (no SSH key needed):
aws ssm start-session --target <instance-id> --region us-east-1
```

### Restart Service

```bash
sudo systemctl restart cdc-service
```

### View Logs

```bash
sudo tail -f /var/log/cdc/cdc-service.log
```

### Change Control State

```bash
# Stop replication (service stays up):
echo '{"state": "stopped"}' | sudo tee /opt/cdc/control.json

# Start replication:
echo '{"state": "running"}' | sudo tee /opt/cdc/control.json
```

### Drop Load Test Artifacts

```bash
set -a && source /opt/cdc/.env && set +a
psql "$SOURCE_DSN" -c "
DROP TABLE IF EXISTS sample_order_items, sample_payments, sample_shipments,
  sample_orders, sample_inventory, sample_products, sample_customers CASCADE;
SELECT pg_drop_replication_slot('sample_cdc_slot')
  FROM pg_replication_slots WHERE slot_name = 'sample_cdc_slot';
DROP PUBLICATION IF EXISTS sample_cdc_pub;
"
rm -f /tmp/loadtest_tables.json /tmp/loadtest_output.log
```

---

## Project Structure

```
pg-dsql-cdc/
├── cdc_service.py              # Main CDC service (streaming consumer, batch writer, health server)
├── load_test_orders.py         # Isolated load test (sample_* tables, own slot)
├── setup_source.py             # One-time source PG setup helper
├── type_mapper.py              # Optional type casting (opt-in via TYPE_MAPPING env)
├── requirements.txt            # Python dependencies (psycopg2-binary, boto3)
├── infra/
│   └── master-stack.yaml       # CloudFormation template (one-click deploy)
├── frontend/
│   ├── index.html              # Dashboard SPA
│   ├── css/style.css           # Light theme styles
│   └── js/app.js               # Dashboard logic
└── api/
    └── lambda_metrics.py       # Reference Lambda code (inline in CFN template)
```

---

## Feedback

Send feedback: ravimat@amazon.com
