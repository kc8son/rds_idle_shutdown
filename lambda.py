import os
import json
import datetime as dt
from typing import Dict, List, Tuple

import boto3
from botocore.exceptions import ClientError

rds = boto3.client("rds")
cw  = boto3.client("cloudwatch")
ssm = boto3.client("ssm")

# Env vars
DEFAULT_LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "20"))
DEFAULT_IDLE_PARAM       = os.getenv("DEFAULT_IDLE_PARAM", "/rds/idle_shutdown_minutes")
REQUIRED_TAG_KEY         = os.getenv("REQUIRED_TAG_KEY", "IdleShutdown")
REQUIRED_TAG_VALUE       = os.getenv("REQUIRED_TAG_VALUE", "enabled")

CPU_PCT_THRESHOLD     = float(os.getenv("CPU_PCT_THRESHOLD", "1.0"))
IOPS_THRESHOLD        = int(os.getenv("IOPS_THRESHOLD", "0"))
CONNECTIONS_THRESHOLD = int(os.getenv("CONNECTIONS_THRESHOLD", "0"))

def _now_utc():
    return dt.datetime.now(dt.timezone.utc)

def _minutes_ago(minutes: int):
    return _now_utc() - dt.timedelta(minutes=minutes)

def get_default_idle_minutes() -> int:
    try:
        resp = ssm.get_parameter(Name=DEFAULT_IDLE_PARAM)
        return int(resp["Parameter"]["Value"])
    except Exception:
        # Fallback if the parameter doesn't exist
        return 30

def list_tagged_db_instances() -> List[Dict]:
    """RDS instances with IdleShutdown=enabled."""
    instances = []
    marker = None
    while True:
        kwargs = {}
        if marker:
            kwargs["Marker"] = marker
        resp = rds.describe_db_instances(**kwargs)
        for dbi in resp["DBInstances"]:
            arn = dbi["DBInstanceArn"]
            tags = rds.list_tags_for_resource(ResourceName=arn)["TagList"]
            if any(t["Key"] == REQUIRED_TAG_KEY and t["Value"].lower() == REQUIRED_TAG_VALUE for t in tags):
                instances.append(dbi)
        marker = resp.get("Marker")
        if not marker:
            break
    return instances

def list_tagged_db_clusters() -> List[Dict]:
    """Aurora clusters with IdleShutdown=enabled."""
    clusters = []
    marker = None
    while True:
        kwargs = {}
        if marker:
            kwargs["Marker"] = marker
        resp = rds.describe_db_clusters(**kwargs)
        for dbc in resp["DBClusters"]:
            arn = dbc["DBClusterArn"]
            tags = rds.list_tags_for_resource(ResourceName=arn)["TagList"]
            if any(t["Key"] == REQUIRED_TAG_KEY and t["Value"].lower() == REQUIRED_TAG_VALUE for t in tags):
                clusters.append(dbc)
        marker = resp.get("Marker")
        if not marker:
            break
    return clusters

def _get_tag_value(tags: List[Dict], key: str):
    for t in tags:
        if t["Key"] == key:
            return t["Value"]
    return None

def get_effective_idle_minutes(resource_arn: str, default_minutes: int) -> int:
    """Use tag IdleMinutes if present, else the SSM default."""
    try:
        tags = rds.list_tags_for_resource(ResourceName=resource_arn)["TagList"]
        override = _get_tag_value(tags, "IdleMinutes")
        if override:
            return int(override)
    except Exception:
        pass
    return default_minutes

def _metric_id(prefix: str, name: str) -> str:
    return f"{prefix}_{name}".replace("/", "_")

def fetch_idle_signals_for_instance(db_instance_id: str, lookback_mins: int) -> Dict[str, float]:
    """
    Summarize recent activity:
      - max DatabaseConnections
      - sum ReadIOPS + sum WriteIOPS
      - max CPUUtilization
    """
    end = _now_utc()
    start = _minutes_ago(lookback_mins)
    period = max(60, (lookback_mins * 60) // 10)  # ~10 datapoints

    queries = []
    def add_metric(metric_name, stat, id_suffix):
        queries.append({
            "Id": _metric_id("m", id_suffix),
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/RDS",
                    "MetricName": metric_name,
                    "Dimensions": [{"Name": "DBInstanceIdentifier", "Value": db_instance_id}],
                },
                "Period": period,
                "Stat": stat,
            },
            "ReturnData": True
        })

    add_metric("DatabaseConnections", "Maximum", "conn_max")
    add_metric("ReadIOPS", "Sum", "read_sum")
    add_metric("WriteIOPS", "Sum", "write_sum")
    add_metric("CPUUtilization", "Maximum", "cpu_max")

    resp = cw.get_metric_data(
        MetricDataQueries=queries,
        StartTime=start,
        EndTime=end,
        ScanBy="TimestampDescending",
    )

    out = {"m_conn_max": 0.0, "m_read_sum": 0.0, "m_write_sum": 0.0, "m_cpu_max": 0.0}
    for r in resp["MetricDataResults"]:
        if r["Values"]:
            if r["Id"].endswith("conn_max") or r["Id"].endswith("cpu_max"):
                out["m_" + r["Id"].split("_",1)[1]] = max(r["Values"])
            else:
                out["m_" + r["Id"].split("_",1)[1]] = sum(r["Values"])
    return out

def is_instance_idle(db_instance_id: str, lookback_mins: int) -> bool:
    m = fetch_idle_signals_for_instance(db_instance_id, lookback_mins)
    total_iops = (m.get("m_read_sum", 0.0) + m.get("m_write_sum", 0.0))
    return (
        m.get("m_conn_max", 0.0) <= CONNECTIONS_THRESHOLD and
        total_iops <= IOPS_THRESHOLD and
        m.get("m_cpu_max", 0.0) <= CPU_PCT_THRESHOLD
    )

def stop_instance(db_instance_id: str) -> Tuple[bool, str]:
    try:
        rds.stop_db_instance(DBInstanceIdentifier=db_instance_id)
        return True, f"Stop initiated for instance {db_instance_id}"
    except ClientError as e:
        return False, f"Could not stop instance {db_instance_id}: {e.response.get('Error', {}).get('Message', str(e))}"

def start_instance(db_instance_id: str) -> Tuple[bool, str]:
    try:
        rds.start_db_instance(DBInstanceIdentifier=db_instance_id)
        return True, f"Start initiated for instance {db_instance_id}"
    except ClientError as e:
        return False, f"Could not start instance {db_instance_id}: {e.response.get('Error', {}).get('Message', str(e))}"

def stop_cluster(db_cluster_id: str) -> Tuple[bool, str]:
    try:
        rds.stop_db_cluster(DBClusterIdentifier=db_cluster_id)
        return True, f"Stop initiated for cluster {db_cluster_id}"
    except ClientError as e:
        return False, f"Could not stop cluster {db_cluster_id}: {e.response.get('Error', {}).get('Message', str(e))}"

def start_cluster(db_cluster_id: str) -> Tuple[bool, str]:
    try:
        rds.start_db_cluster(DBClusterIdentifier=db_cluster_id)
        return True, f"Start initiated for cluster {db_cluster_id}"
    except ClientError as e:
        return False, f"Could not start cluster {db_cluster_id}: {e.response.get('Error', {}).get('Message', str(e))}"

def handle_check(event, context):
    default_idle = get_default_idle_minutes()
    lookback_mins = int(os.getenv("LOOKBACK_MINUTES", str(DEFAULT_LOOKBACK_MINUTES)))
    actions = []

    # Standalone instances
    for dbi in list_tagged_db_instances():
        dbid = dbi["DBInstanceIdentifier"]
        arn  = dbi["DBInstanceArn"]
        status = dbi["DBInstanceStatus"]
        if status != "available":
            actions.append(f"Skip {dbid}: status={status}")
            continue
        if "DBClusterIdentifier" in dbi:
            actions.append(f"Skip {dbid}: part of cluster {dbi['DBClusterIdentifier']}")
            continue

        idle_window = get_effective_idle_minutes(arn, default_idle)
        if is_instance_idle(dbid, min(idle_window, lookback_mins)):
            ok, msg = stop_instance(dbid)
            actions.append(msg)
        else:
            actions.append(f"Keep running {dbid}: not idle")

    # Aurora clusters (decide via writer instance)
    for dbc in list_tagged_db_clusters():
        cluster_id = dbc["DBClusterIdentifier"]
        status = dbc.get("Status")
        if status not in ("available", "in-sync"):
            actions.append(f"Skip cluster {cluster_id}: status={status}")
            continue

        arn = dbc["DBClusterArn"]
        idle_window = get_effective_idle_minutes(arn, default_idle)

        writer_inst = None
        for m in dbc.get("DBClusterMembers", []):
            if m.get("IsClusterWriter"):
                writer_inst = m.get("DBInstanceIdentifier")
                break

        if not writer_inst:
            actions.append(f"Skip cluster {cluster_id}: no writer found")
            continue

        if is_instance_idle(writer_inst, min(idle_window, lookback_mins)):
            ok, msg = stop_cluster(cluster_id)
            actions.append(msg)
        else:
            actions.append(f"Keep running cluster {cluster_id}: not idle (writer={writer_inst})")

    return {"actions": actions}

def handle_start(event, context):
    # API Gateway HTTP API (v2.0) passes query params here
    params = event.get("queryStringParameters") or {}
    resource = params.get("resource") or params.get("db") or ""
    if not resource:
        return _http(400, {"error": "missing resource parameter"})

    if resource.startswith("cluster:"):
        cluster_id = resource.split("cluster:", 1)[1]
        ok, msg = start_cluster(cluster_id)
        code = 200 if ok else 400
        return _http(code, {"message": msg, "resource": cluster_id, "type": "cluster"})
    else:
        inst_id = resource
        ok, msg = start_instance(inst_id)
        code = 200 if ok else 400
        return _http(code, {"message": msg, "resource": inst_id, "type": "instance"})

def _http(status: int, body: Dict):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body)
    }

def lambda_handler(event, context):
    """
    - EventBridge schedule -> source=aws.events -> mode 'check'
    - API Gateway HTTP API  -> requestContext present -> mode 'start'
    - Manual test           -> you can pass {"mode":"check"}
    """
    mode = None
    if isinstance(event, dict):
        mode = event.get("mode")
        if (not mode) and event.get("source") == "aws.events":
            mode = "check"
        if (not mode) and "requestContext" in event:
            mode = "start"

    if mode == "check":
        result = handle_check(event, context)
        print(json.dumps(result))
        return result
    elif mode == "start":
        return handle_start(event, context)
    else:
        result = handle_check(event, context)
        print(json.dumps(result))
        return result
