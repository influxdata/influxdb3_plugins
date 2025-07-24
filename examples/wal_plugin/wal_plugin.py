"""
{
    "plugin_type": ["onwrite"],
    "onwrite_args_config": [
        {
            "name": "double_count_table",
            "example": "temperature",
            "description": "Table name for which to double the row count in write reports.",
            "required": false
        }
    ]
}
"""

# Example WAL plugin that monitors data writes and creates summary reports.
# For each table batch in a WAL flush, this plugin writes a row count summary
# to the 'write_reports' table. The plugin accepts an optional argument to
# double the reported count for a specific table, demonstrating how plugins
# can use trigger arguments to modify behavior.

def process_writes(influxdb3_local, table_batches, args=None):
    for table_batch in table_batches:
        # Skip if table_name is write_reports
        if table_batch["table_name"] == "write_reports":
            continue

        row_count = len(table_batch["rows"])

        # Double row count if table name matches args table_name
        if args and "double_count_table" in args and table_batch["table_name"] == args["double_count_table"]:
            row_count *= 2

        line = LineBuilder("write_reports")\
            .tag("table_name", table_batch["table_name"])\
            .int64_field("row_count", row_count)
        influxdb3_local.write(line)

    influxdb3_local.info("wal_plugin.py done")
