"""
CDC Dashboard API — Lambda Handler
Provides /metrics, /health, and /config endpoints for the CDC dashboard.
Deployed behind API Gateway as a single Lambda function with path-based routing.
"""

import json
import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

# ─── Configuration ────────────────────────────────────────────────────────────

METRIC_NAMESPACE = os.environ.get("METRIC_NAMESPACE", "CDC/PgToDSQL")
CDC_INSTANCE_ID = os.environ.get("CDC_INSTANCE_ID", "")
CDC_HEALTH_PORT = os.environ.get("CDC_HEALTH_PORT", "8080")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
METRIC_PERIOD = int(os.environ.get("METRIC_PERIOD", "60"))  # seconds
LOOKBACK_HOURS = int(os.environ.get("LOOKBACK_HOURS", "1"))

# Allowed CORS origins (set to your CloudFront domain)
CORS_ORIGIN = os.environ.get("CORS_ORIGIN", "*")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients (reused across invocations)
cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)
ec2 = boto3.client("ec2", region_name=AWS_REGION)
ssm = boto3.client("ssm", region_name=AWS_REGION)
logs_client = boto3.client("logs", region_name=AWS_REGION)


# ─── Lambda Handler ──────────────────────────────────────────────────────────

def lambda_handler(event: dict, context: Any) -> dict:
    """Main entry point — routes based on HTTP method + path."""
    http_method = event.get("httpMethod", event.get("requestContext", {}).get("http", {}).get("method", "GET"))
    path = event.get("path", event.get("rawPath", "/"))

    logger.info(json.dumps({"action": "request", "method": http_method, "path": path}))

    try:
        if path.endswith("/metrics") and http_method == "GET":
            body = handle_get_metrics()
        elif path.endswith("/health") and http_method == "GET":
            body = handle_get_health()
        elif path.endswith("/config") and http_method == "POST":
            request_body = json.loads(event.get("body", "{}") or "{}")
            body = handle_post_config(request_body)
        elif path.endswith("/loadtest") and http_method == "POST":
            request_body = json.loads(event.get("body", "{}") or "{}")
            body = handle_post_loadtest(request_body)
        elif "/loadtest/status" in path and http_method == "GET":
            query_params = event.get("queryStringParameters", {}) or {}
            body = handle_get_loadtest_status(query_params)
        elif path.endswith("/table-mapping") and http_method == "POST":
            request_body = json.loads(event.get("body", "{}") or "{}")
            body = handle_post_table_mapping(request_body)
        elif path.endswith("/table-mapping") and http_method == "GET":
            body = handle_get_table_mapping()
        elif path.endswith("/control") and http_method == "POST":
            request_body = json.loads(event.get("body", "{}") or "{}")
            body = handle_post_control(request_body)
        else:
            return response(404, {"error": "Not found", "path": path})

        return response(200, body)

    except ClientError as e:
        logger.error(json.dumps({"error": str(e), "code": e.response["Error"]["Code"]}))
        return response(500, {"error": "AWS service error", "detail": str(e)})
    except Exception as e:
        logger.error(json.dumps({"error": str(e)}))
        return response(500, {"error": "Internal server error"})


# ─── GET /metrics ─────────────────────────────────────────────────────────────

def handle_get_metrics() -> dict:
    """Fetch CloudWatch metrics for the CDC pipeline — last 1 hour."""
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=LOOKBACK_HOURS)

    metrics_to_fetch = [
        {"name": "ReplicationLagBytes", "stat": "Average"},
        {"name": "EventsAppliedPerSecond", "stat": "Average"},
        {"name": "EventsFailed", "stat": "Sum"},
        {"name": "BatchQueueDepth", "stat": "Average"},
        {"name": "CheckpointLSN", "stat": "Maximum"},
    ]

    # Build GetMetricData queries
    metric_queries = []
    for i, m in enumerate(metrics_to_fetch):
        metric_queries.append({
            "Id": f"m{i}",
            "MetricStat": {
                "Metric": {
                    "Namespace": METRIC_NAMESPACE,
                    "MetricName": m["name"],
                },
                "Period": METRIC_PERIOD,
                "Stat": m["stat"],
            },
            "ReturnData": True,
        })

    result = cloudwatch.get_metric_data(
        MetricDataQueries=metric_queries,
        StartTime=start_time,
        EndTime=end_time,
        ScanBy="TimestampAscending",
    )

    # Parse results into timeseries + latest values
    timeseries = {}
    latest = {}

    for i, m in enumerate(metrics_to_fetch):
        metric_result = result["MetricDataResults"][i]
        timestamps = metric_result.get("Timestamps", [])
        values = metric_result.get("Values", [])

        datapoints = [
            {"timestamp": ts.isoformat(), "value": val}
            for ts, val in zip(timestamps, values)
        ]
        timeseries[m["name"]] = datapoints

        if values:
            latest[m["name"]] = values[-1]

    # Compute total events applied (sum over period)
    tps_values = [dp["value"] for dp in timeseries.get("EventsAppliedPerSecond", [])]
    latest["EventsAppliedTotal"] = int(sum(tps_values) * METRIC_PERIOD) if tps_values else 0

    # Fetch recent log events for the event log panel
    events = fetch_recent_events()

    # Table status (derived from CloudWatch dimensions if available)
    tables = fetch_table_status()

    return {
        "timeseries": timeseries,
        "latest": latest,
        "events": events,
        "tables": tables,
        "period": METRIC_PERIOD,
        "startTime": start_time.isoformat(),
        "endTime": end_time.isoformat(),
    }


# ─── GET /health ──────────────────────────────────────────────────────────────

def handle_get_health() -> dict:
    """Fetch CDC health by calling the EC2 instance's /health endpoint via SSM or direct HTTP."""
    import urllib.request
    import urllib.error
    
    try:
        # Get instance private IP
        instance_id = CDC_INSTANCE_ID
        if not instance_id:
            return {"status": "error", "message": "CDC_INSTANCE_ID not configured"}
        
        response = ec2.describe_instances(InstanceIds=[instance_id])
        reservations = response.get("Reservations", [])
        if not reservations or not reservations[0].get("Instances"):
            return {"status": "error", "message": "Instance not found"}
        
        instance = reservations[0]["Instances"][0]
        private_ip = instance.get("PrivateIpAddress", "")
        state = instance.get("State", {}).get("Name", "unknown")
        
        if state != "running":
            return {"status": "stopped", "instance_state": state, "instance_id": instance_id}
        
        # Call the health endpoint directly (Lambda is in the same VPC)
        health_url = f"http://{private_ip}:{CDC_HEALTH_PORT}/health"
        req = urllib.request.Request(health_url, method="GET")
        req.add_header("Accept", "application/json")
        
        with urllib.request.urlopen(req, timeout=5) as resp:
            health_data = json.loads(resp.read().decode())
        
        # Enrich with instance metadata
        health_data["instance_id"] = instance_id
        health_data["instance_state"] = state
        health_data["private_ip"] = private_ip
        
        return health_data
        
    except urllib.error.URLError as e:
        return {
            "status": "unreachable",
            "message": f"CDC service not responding: {str(e)}",
            "instance_id": CDC_INSTANCE_ID,
        }
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return {"status": "error", "message": str(e)}

# ─── POST /config ─────────────────────────────────────────────────────────────

def handle_post_config(body: dict) -> dict:
    """Update ECS service configuration (environment variables)."""
    batch_size = body.get("batch_size")
    conflict_mode = body.get("conflict_mode")

    if batch_size is not None and (batch_size < 1 or batch_size > 50000):
        return {"error": "batch_size must be between 1 and 50000"}

    valid_modes = ["last_write_wins", "source_wins", "skip", "error"]
    if conflict_mode is not None and conflict_mode not in valid_modes:
        return {"error": f"conflict_mode must be one of: {valid_modes}"}

    # Get current task definition to modify environment
    svc_response = ecs.describe_services(cluster=CDC_INSTANCE_ID, services=[CDC_HEALTH_PORT])
    services = svc_response.get("services", [])
    if not services:
        return {"error": "ECS service not found"}

    task_def_arn = services[0]["taskDefinition"]
    task_def = ecs.describe_task_definition(taskDefinition=task_def_arn)["taskDefinition"]

    # Update container environment variables
    container_defs = task_def["containerDefinitions"]
    for container in container_defs:
        env = {e["name"]: e["value"] for e in container.get("environment", [])}
        if batch_size is not None:
            env["CDC_BATCH_SIZE"] = str(batch_size)
        if conflict_mode is not None:
            env["CDC_CONFLICT_MODE"] = conflict_mode
        container["environment"] = [{"name": k, "value": v} for k, v in env.items()]

    # Register new task definition revision
    register_params = {
        "family": task_def["family"],
        "containerDefinitions": container_defs,
        "taskRoleArn": task_def.get("taskRoleArn", ""),
        "executionRoleArn": task_def.get("executionRoleArn", ""),
        "networkMode": task_def.get("networkMode", "awsvpc"),
        "cpu": task_def.get("cpu"),
        "memory": task_def.get("memory"),
        "requiresCompatibilities": task_def.get("requiresCompatibilities", ["FARGATE"]),
    }
    # Remove None values
    register_params = {k: v for k, v in register_params.items() if v}

    new_task_def = ecs.register_task_definition(**register_params)
    new_arn = new_task_def["taskDefinition"]["taskDefinitionArn"]

    # Update service to use new task definition (triggers rolling deploy)
    ecs.update_service(
        cluster=CDC_INSTANCE_ID,
        service=CDC_HEALTH_PORT,
        taskDefinition=new_arn,
        forceNewDeployment=True,
    )

    logger.info(json.dumps({
        "action": "config_updated",
        "batch_size": batch_size,
        "conflict_mode": conflict_mode,
        "new_task_def": new_arn,
    }))

    return {
        "success": True,
        "message": "Configuration updated. Service redeploying.",
        "taskDefinition": new_arn.split("/")[-1],
    }




# ─── POST /loadtest ─────────────────────────────────────────────────────────────

SSM_CLIENT = boto3.client("ssm", region_name=AWS_REGION)
S3_CLIENT = boto3.client("s3", region_name=AWS_REGION)
CDC_INSTANCE_ID = os.environ.get("CDC_INSTANCE_ID", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "")

# Store active command ID (in-memory for Lambda — consider DynamoDB for prod)
_active_command_id = None


def handle_post_loadtest(body: dict) -> dict:
    """Trigger a load test on the CDC EC2 instance via SSM RunCommand."""
    global _active_command_id

    if not CDC_INSTANCE_ID:
        return {"error": "CDC_INSTANCE_ID not configured"}

    duration = body.get("duration", 300)
    orders_per_sec = body.get("orders_per_sec", 20)
    threads = body.get("threads", 4)
    mode = body.get("mode", "mixed")

    # Build the command
    command = (
        f"set -a && source /opt/cdc/.env && set +a && "
        f"cd /opt/cdc && python3.11 /opt/cdc/load_test_orders.py "
        f"--source-dsn \"$SOURCE_DSN\" "
        f"--target-dsn \"host=$DSQL_HOSTNAME port=5432 dbname=postgres user=admin "
        f"password=$(aws dsql generate-db-connect-admin-auth-token "
        f"--hostname $DSQL_HOSTNAME --region $DSQL_REGION --expires-in 900) sslmode=require\" "
        f"--duration {int(duration)} "
        f"--orders-per-sec {int(orders_per_sec)} "
        f"--threads {int(threads)} "
        f"2>&1 | tee /tmp/loadtest_output.log"
    )

    try:
        result = SSM_CLIENT.send_command(
            InstanceIds=[CDC_INSTANCE_ID],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": [command]},
            TimeoutSeconds=min(int(duration) + 120, 3600),
            Comment=f"CDC Load Test: {orders_per_sec} orders/s for {duration}s",
        )
        command_id = result["Command"]["CommandId"]
        _active_command_id = command_id

        logger.info(json.dumps({
            "action": "loadtest_started",
            "command_id": command_id,
            "duration": duration,
            "orders_per_sec": orders_per_sec,
        }))

        return {
            "status": "started",
            "command_id": command_id,
            "message": f"Load test started: {orders_per_sec} orders/s for {duration}s",
        }
    except ClientError as e:
        return {"error": f"Failed to start load test: {str(e)}"}


def handle_get_loadtest_status(query_params: dict) -> dict:
    """Check the status of a running load test via SSM command invocation."""
    command_id = query_params.get("command_id", _active_command_id)
    if not command_id:
        return {"status": "idle", "message": "No active load test"}

    if not CDC_INSTANCE_ID:
        return {"error": "CDC_INSTANCE_ID not configured"}

    try:
        result = SSM_CLIENT.get_command_invocation(
            CommandId=command_id,
            InstanceId=CDC_INSTANCE_ID,
        )

        status = result["Status"]  # Pending, InProgress, Success, Failed, Cancelled
        output = result.get("StandardOutputContent", "")
        error_output = result.get("StandardErrorContent", "")

        # Parse progress from output
        progress = parse_loadtest_progress(output)

        if status in ("Success",):
            # Try to read results from the output
            results = parse_loadtest_results(output)
            return {
                "status": "complete",
                "output": output[-5000:],  # Last 5KB
                "progress": progress,
                "results": results,
            }
        elif status in ("Failed", "Cancelled", "TimedOut"):
            return {
                "status": "failed",
                "output": output[-5000:],
                "error": error_output[:2000] or f"Command {status}",
            }
        else:
            # Still running
            return {
                "status": "running",
                "output": output[-5000:],
                "progress": progress,
            }

    except ClientError as e:
        if "InvocationDoesNotExist" in str(e):
            return {"status": "pending", "message": "Command still initializing..."}
        return {"error": str(e)}


def parse_loadtest_progress(output: str) -> dict:
    """Extract progress metrics from load test output."""
    progress = {}
    lines = output.split("\n")

    for line in reversed(lines):
        if "Orders placed:" in line:
            try:
                parts = line.split("Orders placed:")[1].split(",")
                progress["orders_placed"] = int(parts[0].strip().replace(",", ""))
            except (IndexError, ValueError):
                pass
        if "DML ops:" in line:
            try:
                progress["total_dml_operations"] = int(
                    line.split("DML ops:")[1].split(",")[0].strip().replace(",", "")
                )
            except (IndexError, ValueError):
                pass
        if "Effective TPS:" in line:
            try:
                progress["effective_tps"] = float(
                    line.split("Effective TPS:")[1].strip().split()[0]
                )
            except (IndexError, ValueError):
                pass
        if "Errors:" in line and "error" not in line.lower()[:10]:
            try:
                progress["errors"] = int(
                    line.split("Errors:")[1].strip().split()[0].replace(",", "")
                )
            except (IndexError, ValueError):
                pass

    return progress


def parse_loadtest_results(output: str) -> dict:
    """Extract final results and integrity data from load test output."""
    results = {"integrity": {}}

    lines = output.split("\n")
    for line in lines:
        # Match table integrity lines like: "  ✓ customers   source=500  target=500  OK"
        if ("source=" in line and "target=" in line) or ("✓" in line and "source=" in line) or ("✗" in line and "source=" in line):
            parts = line.strip().split()
            for i, part in enumerate(parts):
                if part in ("✓", "✗"):
                    table_name = parts[i + 1] if i + 1 < len(parts) else ""
                    source = 0
                    target = 0
                    for p in parts:
                        if p.startswith("source="):
                            try:
                                source = int(p.split("=")[1].replace(",", ""))
                            except ValueError:
                                pass
                        if p.startswith("target="):
                            try:
                                target = int(p.split("=")[1].replace(",", ""))
                            except ValueError:
                                pass
                    results["integrity"][table_name] = {
                        "source": source,
                        "target": target,
                        "match": source == target,
                        "diff": source - target,
                    }
                    break

    return results

# ─── POST /table-mapping ─────────────────────────────────────────────────────────────

def handle_post_table_mapping(body: dict) -> dict:
    """Apply DMS-style table mapping rules to the CDC service via SSM."""
    if not CDC_INSTANCE_ID:
        return {"error": "CDC_INSTANCE_ID not configured"}

    rules = body.get("rules")
    if rules is None or not isinstance(rules, list):
        return {"error": "Request body must contain a 'rules' array"}

    # Validate basic structure
    for i, rule in enumerate(rules):
        if rule.get("rule-type") != "selection":
            return {"error": f"Rule {i+1}: rule-type must be 'selection'"}
        locator = rule.get("object-locator", {})
        if not locator.get("schema-name") or not locator.get("table-name"):
            return {"error": f"Rule {i+1}: object-locator must have schema-name and table-name"}
        if rule.get("rule-action") not in ("include", "exclude"):
            return {"error": f"Rule {i+1}: rule-action must be 'include' or 'exclude'"}

    import base64
    rules_json = json.dumps(body, indent=2)
    b64_rules = base64.b64encode(rules_json.encode()).decode()

    command = f"echo '{b64_rules}' | base64 -d > /opt/cdc/table_rules.json && systemctl restart cdc-service"

    try:
        result = ssm.send_command(
            InstanceIds=[CDC_INSTANCE_ID],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": [command]},
            TimeoutSeconds=30,
            Comment="Apply table mapping rules",
        )
        command_id = result["Command"]["CommandId"]
        logger.info(json.dumps({"action": "table_mapping_applied", "rules_count": len(rules), "command_id": command_id}))

        return {
            "status": "applied",
            "rules_count": len(rules),
            "command_id": command_id,
            "message": f"Applied {len(rules)} table mapping rules. Service restarting.",
        }
    except ClientError as e:
        return {"error": f"Failed to apply rules: {str(e)}"}


# ─── GET /table-mapping ──────────────────────────────────────────────────────────────

def handle_get_table_mapping() -> dict:
    """Read current table mapping rules from the CDC instance."""
    if not CDC_INSTANCE_ID:
        return {"error": "CDC_INSTANCE_ID not configured"}

    try:
        result = ssm.send_command(
            InstanceIds=[CDC_INSTANCE_ID],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": ["cat /opt/cdc/table_rules.json 2>/dev/null || echo '{}'"]},
            TimeoutSeconds=10,
            Comment="Read table mapping rules",
        )
        command_id = result["Command"]["CommandId"]

        # Wait briefly for the command to complete
        import time
        time.sleep(2)

        invocation = ssm.get_command_invocation(
            CommandId=command_id,
            InstanceId=CDC_INSTANCE_ID,
        )

        if invocation["Status"] == "Success":
            output = invocation.get("StandardOutputContent", "{}").strip()
            try:
                return json.loads(output)
            except json.JSONDecodeError:
                return {}
        else:
            return {"error": f"Command status: {invocation['Status']}"}

    except ClientError as e:
        if "InvocationDoesNotExist" in str(e):
            # Command still running, return empty
            return {}
        return {"error": f"Failed to read rules: {str(e)}"}


# ─── POST /control ───────────────────────────────────────────────────────────────────

def handle_post_control(body: dict) -> dict:
    """Control the CDC replication service (start/pause/resume/stop)."""
    if not CDC_INSTANCE_ID:
        return {"error": "CDC_INSTANCE_ID not configured"}

    action = body.get("action", "").lower()
    valid_actions = ("start", "pause", "resume", "stop")
    if action not in valid_actions:
        return {"error": f"action must be one of: {valid_actions}"}

    # Map action to control state
    state_map = {
        "start": "running",
        "resume": "running",
        "pause": "paused",
        "stop": "stopped",
    }
    new_state = state_map[action]

    # Build command: write control file + optionally start service
    control_json_str = json.dumps({"state": new_state})
    commands = ["echo '" + control_json_str + "' > /opt/cdc/control.json"]
    if action == "start":
        commands.append("systemctl start cdc-service 2>/dev/null || true")
    elif action == "stop":
        # Give the service a moment to see the control file before systemctl
        commands.append("sleep 5 && systemctl stop cdc-service 2>/dev/null || true")

    try:
        result = ssm.send_command(
            InstanceIds=[CDC_INSTANCE_ID],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": commands},
            TimeoutSeconds=30,
            Comment=f"CDC control: {action}",
        )
        command_id = result["Command"]["CommandId"]
        logger.info(json.dumps({"action": "control", "new_state": new_state, "command_id": command_id}))

        return {
            "status": new_state,
            "action": action,
            "command_id": command_id,
        }
    except ClientError as e:
        return {"error": f"Failed to {action}: {str(e)}"}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def fetch_recent_events() -> list:
    """Fetch recent CDC log events from CloudWatch Logs."""
    log_group = os.environ.get("LOG_GROUP", f"/ecs/{CDC_HEALTH_PORT}")
    try:
        result = logs_client.filter_log_events(
            logGroupName=log_group,
            limit=50,
            interleaved=True,
        )
        events = []
        for event in result.get("events", []):
            msg = event.get("message", "")
            try:
                parsed = json.loads(msg)
                events.append({
                    "timestamp": event["timestamp"],
                    "level": parsed.get("level", "info").lower(),
                    "message": parsed.get("message", msg),
                })
            except (json.JSONDecodeError, KeyError):
                level = "error" if "ERROR" in msg else "warn" if "WARN" in msg else "info"
                events.append({
                    "timestamp": event["timestamp"],
                    "level": level,
                    "message": msg[:200],
                })
        return events
    except ClientError:
        return []


def fetch_table_status() -> list:
    """Fetch per-table metrics if available via CloudWatch dimensions."""
    tables_env = os.environ.get("CDC_TABLES", "")
    if not tables_env:
        return []

    table_names = [t.strip() for t in tables_env.split(",") if t.strip()]
    tables = []

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=5)

    for table in table_names:
        try:
            result = cloudwatch.get_metric_data(
                MetricDataQueries=[
                    {
                        "Id": "lag",
                        "MetricStat": {
                            "Metric": {
                                "Namespace": METRIC_NAMESPACE,
                                "MetricName": "ReplicationLagBytes",
                                "Dimensions": [{"Name": "TableName", "Value": table}],
                            },
                            "Period": 60,
                            "Stat": "Average",
                        },
                        "ReturnData": True,
                    },
                ],
                StartTime=start_time,
                EndTime=end_time,
            )
            lag_values = result["MetricDataResults"][0].get("Values", [])
            lag = lag_values[-1] if lag_values else 0
            status = "active" if lag < 1024 * 1024 else "lagging" if lag < 10 * 1024 * 1024 else "behind"

            tables.append({
                "name": table,
                "status": status,
                "lag": lag,
                "lastEvent": end_time.isoformat(),
                "eventsApplied": 0,  # Would need per-table metric
                "errors": 0,
            })
        except ClientError:
            tables.append({"name": table, "status": "unknown", "lag": 0, "errors": 0})

    return tables



# ─── POST /table-mapping ─────────────────────────────────────────────────

def handle_post_table_mapping(body: dict) -> dict:
    """Apply DMS-style table mapping rules to the CDC service."""
    import base64
    
    ssm = boto3.client("ssm")
    instance_id = os.environ.get("CDC_INSTANCE_ID", "")
    
    # Validate rules structure
    rules = body.get("rules", [])
    if not isinstance(rules, list):
        return {"error": "JSON must have a 'rules' array"}
    
    selection_count = sum(1 for r in rules if r.get("rule-type") == "selection")
    
    # Write rules to EC2 via SSM
    rules_json = json.dumps(body, indent=2)
    b64_rules = base64.b64encode(rules_json.encode()).decode()
    
    cmd = f"echo {b64_rules} | base64 -d > /opt/cdc/table_rules.json && systemctl restart cdc-service"
    
    resp = ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [cmd]},
        TimeoutSeconds=30,
    )
    
    return {"status": "applied", "rules_count": selection_count, "command_id": resp["Command"]["CommandId"]}


# ─── GET /table-mapping ──────────────────────────────────────────────────

def handle_get_table_mapping() -> dict:
    """Read current table mapping rules from the CDC instance."""
    ssm = boto3.client("ssm")
    instance_id = os.environ.get("CDC_INSTANCE_ID", "")
    
    try:
        resp = ssm.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": ["cat /opt/cdc/table_rules.json 2>/dev/null || echo '{}'"]},
            TimeoutSeconds=10,
        )
        command_id = resp["Command"]["CommandId"]
        
        # Wait briefly for the command to complete
        import time
        time.sleep(2)
        
        inv = ssm.get_command_invocation(CommandId=command_id, InstanceId=instance_id)
        if inv["Status"] == "Success":
            content = inv.get("StandardOutputContent", "{}")
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                return {"content": content}
        return {"error": "Command not completed yet", "status": inv["Status"]}
    except Exception as e:
        return {"error": str(e), "rules": []}


# ─── POST /control ───────────────────────────────────────────────────────

def handle_post_control(body: dict) -> dict:
    """Control CDC replication: start, pause, resume, stop."""
    ssm = boto3.client("ssm")
    instance_id = os.environ.get("CDC_INSTANCE_ID", "")
    
    action = body.get("action", body.get("state", ""))
    state_map = {"start": "running", "resume": "running", "pause": "paused", "stop": "stopped"}
    new_state = state_map.get(action, action)
    
    if new_state not in ("running", "paused", "stopped"):
        return {"error": f"Invalid action: {action}. Use: start, pause, resume, stop"}
    
    # Write control file via SSM
    control_json = json.dumps({"state": new_state})
    cmd = f"echo '{control_json}' > /opt/cdc/control.json"
    
    # If starting from stopped state, also start the systemd service
    if new_state == "running" and action in ("start", "resume"):
        cmd += " && systemctl start cdc-service 2>/dev/null || true"
    
    resp = ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [cmd]},
        TimeoutSeconds=10,
    )
    
    return {"status": "ok", "state": new_state, "command_id": resp["Command"]["CommandId"]}



def response(status_code: int, body: dict) -> dict:
    """Build API Gateway proxy response with CORS headers."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": CORS_ORIGIN,
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
        },
        "body": json.dumps(body, default=str),
    }
