"""
{
    "plugin_type": ["onwrite"],
    "onwrite_args_config": [
        {
            "name": "host",
            "example": "https://remote-influxdb.com:8181",
            "description": "Remote InfluxDB host URL.",
            "required": true
        },
        {
            "name": "token",
            "example": "remote_api_token",
            "description": "Remote InfluxDB API token.",
            "required": true
        },
        {
            "name": "database",
            "example": "remote_db",
            "description": "Remote database name.",
            "required": true
        },
        {
            "name": "tables",
            "example": "cpu,mem,disk",
            "description": "Comma-separated list of tables to replicate. If not specified, all tables will be replicated.",
            "required": false
        },
        {
            "name": "aggregate_interval",
            "example": "10s",
            "description": "Aggregation interval for downsampling in seconds (e.g., '1a', '5s', '10s'). If not specified, no downsampling will be applied.",
            "required": false
        },
        {
            "name": "validate",
            "example": "true",
            "description": "Enable validation of replicated data by comparing checksums. Defaults to 'false'.",
            "required": false
        }
    ]
}
"""
# Data Replicator Plugin for InfluxDB v3 Core/Enterprise
# Copyright (c) 2025 InfluxData Inc.

import os
import json
import time
import hashlib
from pathlib import Path
from collections import defaultdict
from influxdb_client_3 import InfluxDBClient3, InfluxDBError

# Configuration
try:
    PLUGIN_DIR = Path(__file__).parent
except NameError:
    PLUGIN_DIR = Path(os.getenv("PLUGIN_DIR", os.path.expanduser("~/.plugins")))
QUEUE_FILE = PLUGIN_DIR / "edr_queue.jsonl"  # Plain text queue file (no compression)
STATE_KEY = "last_replicated_timestamp"  # Cache key for tracking replication progress

# Custom timestamp (in nanoseconds) for testing
# 2025-03-31T12:00:00Z = 1743441600000000000 nanoseconds
CUSTOM_TIMESTAMP_NS = 1743441600000000000


def ensure_queue_file():
    """Ensure the queue file directory exists."""
    if not QUEUE_FILE.parent.exists():
        QUEUE_FILE.parent.mkdir(parents=True)


def append_to_queue(entries):
    """Append entries to the queue file (JSON-serializable data only)."""
    ensure_queue_file()
    with open(QUEUE_FILE, "a", encoding="utf-8") as f:
        for entry in entries:
            # Only store serializable fields (table, line, checksum)
            queue_entry = {"table": entry["table"], "line": entry["line"]}
            if "checksum" in entry:
                queue_entry["checksum"] = entry["checksum"]
            f.write(json.dumps(queue_entry) + "\n")


def read_queue():
    """Read all lines from the queue file."""
    ensure_queue_file()
    if not QUEUE_FILE.exists():
        return []
    with open(QUEUE_FILE, "r", encoding="utf-8") as f:
        return [json.loads(line.strip()) for line in f if line.strip()]


def truncate_queue(successful_entries):
    """Remove successfully replicated entries from the queue."""
    all_entries = read_queue()
    remaining_entries = [entry for entry in all_entries if entry not in successful_entries]
    with open(QUEUE_FILE, "w", encoding="utf-8") as f:
        for entry in remaining_entries:
            f.write(json.dumps(entry) + "\n")


def row_to_line_protocol(table_name, row, logger=None):
    """
    Convert a row dictionary to a line protocol string with a custom timestamp.

    Args:
        table_name (str): Measurement name.
        row (dict): Row data with 'time', tags, and fields.
        logger: Logger object to log issues (e.g., influxdb3_local).

    Returns:
        str: Line protocol string, or None if invalid.
    """
    if not row:
        if logger:
            logger.info(f"Skipping row in table {table_name}: row is empty")
        return None

    # Use a custom timestamp instead of the original
    timestamp = CUSTOM_TIMESTAMP_NS

    # Separate tags and fields
    tags = {k: str(v) for k, v in row.items() if k != "time" and v is not None and not isinstance(v, (int, float, bool))}
    fields = {k: v for k, v in row.items() if k != "time" and v is not None and isinstance(v, (int, float, bool))}
    string_fields = {k: str(v) for k, v in row.items() if k != "time" and v is not None and isinstance(v, str) and k not in tags}

    # Format tags
    tag_str = ""
    if tags:
        tag_pairs = [f"{k}={v}" for k, v in sorted(tags.items())]
        tag_str = "," + ",".join(tag_pairs)

    # Format fields
    field_pairs = []
    for k, v in sorted(fields.items()):
        if isinstance(v, bool):
            field_pairs.append(f"{k}={str(v).lower()}")
        elif isinstance(v, int):
            field_pairs.append(f"{k}={v}i")  # Explicitly mark integers
        else:
            field_pairs.append(f"{k}={v}")
    for k, v in sorted(string_fields.items()):
        field_pairs.append(f"{k}=\"{v}\"")

    if not field_pairs:
        if logger:
            logger.info(f"Skipping row in table {table_name}: no fields provided - row: {row}")
        return None

    field_str = ",".join(field_pairs)

    # Construct line protocol with the custom timestamp
    return f"{table_name}{tag_str} {field_str} {timestamp}"


def process_writes(influxdb3_local, table_batches, args=None):
    """
    Replicate any data written to InfluxDB v3 Core to a remote InfluxDB 3 instance on WAL flush,
    with validation, downsampling, and table filtering.

    Args:
        influxdb3_local: Local InfluxDB 3 instance for logging and caching.
        table_batches: List of dictionaries containing table data from WAL flush.
        args: Runtime arguments (host, token, database, tables, aggregate_interval, validate).
    """
    influxdb3_local.info(f"Starting generic data replication process with line protocol, PLUGIN_DIR={PLUGIN_DIR}")

    if not args or "host" not in args or "token" not in args or "database" not in args:
        influxdb3_local.error("Missing required arguments: host, token, or database")
        return

    remote_host = args["host"]
    remote_token = args["token"]
    remote_db = args["database"]

    tables_to_replicate = args.get("tables", "").split(",") if args.get("tables") else None
    aggregate_interval = args.get("aggregate_interval")
    do_validate = args.get("validate", "false").lower() == "true"

    # Log the validation setting for debugging
    influxdb3_local.info(f"Validation enabled: {do_validate}")

    try:
        client = InfluxDBClient3(
            host=remote_host,
            token=remote_token,
            database=remote_db
        )
    except Exception as e:
        influxdb3_local.error(f"Failed to initialize remote client: {str(e)}")
        return

    lines_to_replicate = []
    latest_timestamp = influxdb3_local.cache.get(STATE_KEY, default=0)

    if aggregate_interval:  # Only downsample if aggregate_interval is explicitly set
        interval_sec = 60 if aggregate_interval == "1m" else int(aggregate_interval[:-1])
        aggregates = defaultdict(lambda: {"count": 0, "sum": {}, "tags": {}})

        for table_batch in table_batches:
            table_name = table_batch["table_name"]
            if tables_to_replicate and table_name not in tables_to_replicate:
                continue

            for row in table_batch["rows"]:
                timestamp = row.get("time")
                if not timestamp or timestamp <= latest_timestamp:
                    continue

                # Filter for cpu = 'cpu-total'
                if table_name == "cpu" and row.get("cpu") != "cpu-total":
                    continue

                bucket_ts = timestamp // (interval_sec * 10**9) * (interval_sec * 10**9)
                key = (table_name, bucket_ts)

                aggregates[key]["count"] += 1
                tags = {k: str(v) for k, v in row.items() if k != "time" and v is not None and not isinstance(v, (int, float, bool))}
                fields = {k: v for k, v in row.items() if k != "time" and v is not None and isinstance(v, (int, float))}
                aggregates[key]["tags"] = tags
                for field, value in fields.items():
                    aggregates[key]["sum"][field] = aggregates[key]["sum"].get(field, 0) + value

        for (table_name, timestamp), data in aggregates.items():
            avg_fields = {f"avg_{field}": total / data["count"] for field, total in data["sum"].items()}
            aggregated_row = {"time": timestamp, **data["tags"], **avg_fields}
            line = row_to_line_protocol(table_name, aggregated_row, influxdb3_local)
            if line:
                lines_to_replicate.append({"table": table_name, "line": line})
                latest_timestamp = max(latest_timestamp, timestamp)
    else:
        for table_batch in table_batches:
            table_name = table_batch["table_name"]
            if tables_to_replicate and table_name not in tables_to_replicate:
                continue

            for row in table_batch["rows"]:
                timestamp = row.get("time")
                if not timestamp or timestamp <= latest_timestamp:
                    continue

                # Filter for cpu = 'cpu-total'
                if table_name == "cpu" and row.get("cpu") != "cpu-total":
                    continue

                # Override the timestamp in the row for local write
                row["time"] = CUSTOM_TIMESTAMP_NS
                line = row_to_line_protocol(table_name, row, influxdb3_local)
                if line:
                    lines_to_replicate.append({"table": table_name, "line": line})
                    latest_timestamp = max(latest_timestamp, CUSTOM_TIMESTAMP_NS)

    if lines_to_replicate:
        if do_validate:
            for entry in lines_to_replicate:
                entry["checksum"] = hashlib.md5(entry["line"].encode()).hexdigest()
        append_to_queue(lines_to_replicate)
        influxdb3_local.info(f"Queued {len(lines_to_replicate)} lines from {', '.join(set(p['table'] for p in lines_to_replicate))}")

    queued_entries = read_queue()
    if not queued_entries:
        influxdb3_local.info("No data to replicate")
        return

    max_retries = 3
    successful_entries = []
    for attempt in range(max_retries):
        try:
            # Write line protocol strings directly
            lines = [entry["line"] for entry in queued_entries]
            client.write(lines)  # Write as line protocol
            successful_entries = queued_entries
            influxdb3_local.info(f"Replicated {len(successful_entries)} lines to remote instance")

            if do_validate:
                influxdb3_local.info("Starting validation of replicated entries")
                for entry in successful_entries:
                    expected_checksum = entry.get("checksum")
                    if expected_checksum:
                        # Convert the integer timestamp (nanoseconds) to RFC3339 format
                        timestamp_ns = int(entry["line"].split()[-1])  # Extract timestamp from line protocol
                        # Convert nanoseconds to seconds and microseconds
                        timestamp_sec = timestamp_ns // 1_000_000_000
                        timestamp_ns_remainder = timestamp_ns % 1_000_000_000
                        # Format as RFC3339 (e.g., '2025-03-31T07:43:45.123456789Z')
                        timestamp_rfc3339 = time.strftime(
                            "%Y-%m-%dT%H:%M:%S", time.gmtime(timestamp_sec)
                        ) + f".{timestamp_ns_remainder:09d}Z"
                        # Use the formatted timestamp in the query
                        query = f"SELECT * FROM {entry['table']} WHERE time = '{timestamp_rfc3339}' LIMIT 1"
                        result = client.query(query, language="sql")
                        # result is a pyarrow.Table, so directly convert to Pandas
                        actual_line = result.to_pandas().to_csv(index=False)
                        actual_checksum = hashlib.md5(actual_line.encode()).hexdigest()
                        if actual_checksum != expected_checksum:
                            influxdb3_local.error(f"Validation failed for {entry['table']} at {timestamp_rfc3339}")

            truncate_queue(successful_entries)
            influxdb3_local.cache.put(STATE_KEY, latest_timestamp)
            break
        except InfluxDBError as e:
            if e.response and e.response.status == 429:
                # Handle 429 Too Many Requests
                retry_after = int(e.response.headers.get("retry-after", 2 ** attempt))
                influxdb3_local.info(f"Rate limit hit (429), retrying after {retry_after} seconds")
                time.sleep(retry_after)
            else:
                influxdb3_local.error(f"Replication attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    influxdb3_local.error("Max retries reached; data remains in queue")
                    break
        except Exception as e:
            influxdb3_local.error(f"Replication attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                influxdb3_local.error("Max retries reached; data remains in queue")
                break
