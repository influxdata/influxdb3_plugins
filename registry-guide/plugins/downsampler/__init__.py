"""Plugin entry point for the `process_writes` trigger."""


def process_writes(influxdb3_local, table_batches, args):
    """Called after each WAL commit. `table_batches` is grouped by table."""
    for batch in table_batches:
        influxdb3_local.info(f"received {len(batch.rows)} rows for {batch.table_name}")
