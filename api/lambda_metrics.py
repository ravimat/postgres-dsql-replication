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
ECS_CLUSTER = os.environ.get("ECS_CLUSTER", "cdc-cluster")
ECS_SERVICE = os.environ.get("ECS_SERVICE", "cdc-pg-dsql-service")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
METRIC_PERIOD = int(os.environ.get("METRIC_PERIOD", "60"))  # seconds
LOOKBACK_HOURS = int(os.environ.get("LOOKBACK_HOURS", "1"))

# Allowed CORS origins (set to your CloudFront domain)
CORS_ORIGIN = os.environ.get("CORS_ORIGIN", "*")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients (reused across invocations)
cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)
ecs = boto3.client("ecs", region_name=AWS_REGION)
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
    """Return ECS service health and metadata."""
    try:
        svc_response = ecs.describe_services(
            cluster=ECS_CLUSTER,
            services=[ECS_SERVICE],
        )
        services = svc_response.get("services", [])
        if not services:
            return {"status": "not_found", "cluster": ECS_CLUSTER, "service": ECS_SERVICE}

        svc = services[0]
        running_count = svc.get("runningCount", 0)
        desired_count = svc.get("desiredCount", 0)
        task_def = svc.get("taskDefinition", "").split("/")[-1]

        # Get task start time for uptime calculation
        tasks_response = ecs.list_tasks(cluster=ECS_CLUSTER, serviceName=ECS_SERVICE, desiredStatus="RUNNING")
        task_arns = tasks_response.get("taskArns", [])

        uptime = "--"
        if task_arns:
            task_details = ecs.describe_tasks(cluster=ECS_CLUSTER, tasks=[task_arns[0]])
            tasks = task_details.get("tasks", [])
            if tasks and tasks[0].get("startedAt"):
                started_at = tasks[0]["startedAt"]
                if isinstance(started_at, str):
                    started_at = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
                delta = datetime.now(timezone.utc) - started_at
                hours = int(delta.total_seconds() // 3600)
                minutes = int((delta.total_seconds() % 3600) // 60)
                uptime = f"{hours}h {minutes}m"

        status = "healthy" if running_count >= desired_count and running_count > 0 else "degraded"

        return {
            "status": status,
            "cluster": ECS_CLUSTER,
            "service": ECS_SERVICE,
            "taskDefinition": task_def,
            "runningCount": running_count,
            "desiredCount": desired_count,
            "uptime": uptime,
            "region": AWS_REGION,
        }

    except ClientError as e:
        logger.warning(f"ECS health check failed: {e}")
        return {"status": "unknown", "error": str(e)}


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
    svc_response = ecs.describe_services(cluster=ECS_CLUSTER, services=[ECS_SERVICE])
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
        cluster=ECS_CLUSTER,
        service=ECS_SERVICE,
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


# ─── Helpers ──────────────────────────────────────────────────────────────────

def fetch_recent_events() -> list:
    """Fetch recent CDC log events from CloudWatch Logs."""
    log_group = os.environ.get("LOG_GROUP", f"/ecs/{ECS_SERVICE}")
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
