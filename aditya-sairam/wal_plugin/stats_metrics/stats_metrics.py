import datetime
from collections import defaultdict

def calculate_median(values):
    sorted_values = sorted(values)
    n = len(sorted_values)
    if n % 2 == 1:
        return sorted_values[n // 2]
    else:
        return (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2

def calculate_mode(values):
    freq = {}
    for v in values:
        freq[v] = freq.get(v, 0) + 1
    max_count = max(freq.values())
    modes = [k for k, v in freq.items() if v == max_count]
    return modes[0] if len(modes) == 1 else None  # Return first mode or None if multimodal

def calculate_percentile(values, percentile):
    sorted_values = sorted(values)
    index = int(len(sorted_values) * (percentile / 100))
    return sorted_values[min(index, len(sorted_values) - 1)]  # Ensure valid index

def parse_time_sampling(time_sampling):
    """Parse the time_sampling string (e.g., '10d') into a timedelta."""
    unit = time_sampling[-1]
    value = int(time_sampling[:-1])
    if unit == 'd':
        return datetime.timedelta(days=value)
    elif unit == 'h':
        return datetime.timedelta(hours=value)
    elif unit == 'm':
        return datetime.timedelta(minutes=value)
    elif unit == 's':
        return datetime.timedelta(seconds=value)
    else:
        raise ValueError(f"Unsupported time unit: {unit}")

def parse_timestamp(time_value):
    """Parse a timestamp (ISO string, Unix timestamp in seconds/milliseconds/nanoseconds) into a datetime object."""
    if isinstance(time_value, str):
        # Assume ISO format
        return datetime.datetime.fromisoformat(time_value)
    elif isinstance(time_value, (int, float)):
        # Handle Unix timestamp (in seconds, milliseconds, or nanoseconds)
        if time_value > 1e18:  # Likely in nanoseconds (e.g., 1591780500000000000)
            time_value = time_value / 1e9  # Convert to seconds
        elif time_value > 1e12:  # Likely in milliseconds (e.g., 1591780500000)
            time_value = time_value / 1000  # Convert to seconds
        return datetime.datetime.fromtimestamp(time_value)
    else:
        raise TypeError(f"Unsupported timestamp type: {type(time_value)}")

def process_writes(influxdb3_local, table_batches, args=None):
    time_sampling = args.get("time_sampling") if args else None
    time_bucket = time_sampling.split(" ")[-1] if time_sampling else None
    time_bucket_size = parse_time_sampling(time_bucket) if time_bucket else None

    for table_batch in table_batches:
        table_name = ""
        if args and "table_name" in args:
            table_name = args["table_name"]

        if table_batch["table_name"] == table_name:
            analytics_table = f"analytics_{table_name}"

            try:
                current_stats = influxdb3_local.query(f"SELECT * FROM {analytics_table}")
                influxdb3_local.info("The current data are:", current_stats)
                has_existing_stats = True

                # Ensure we get a dictionary (first entry in the list)
                if isinstance(current_stats, list) and current_stats:
                    current_stats = current_stats[0]  # Take first element if it's a list

            except:
                influxdb3_local.info("No data present!")
                has_existing_stats = False
                current_stats = {"min": {}, "max": {}, "sum": {}, "count": {}}

            new_stats = {"min": {}, "max": {}, "sum": {}, "count": {}}
            field_values_list = {}  # Store all values for advanced calculations

            # Sort rows by the 'time' column
            sorted_rows = sorted(table_batch["rows"], key=lambda row: parse_timestamp(row["time"]))

            # Group data by time buckets if time_sampling is provided
            if time_bucket_size:
                time_buckets = defaultdict(lambda: {"values": defaultdict(list), "stats": {}})

                for row in sorted_rows:  # Process rows in chronological order
                    if "fields" in row:
                        fields_dict = row["fields"]
                    else:
                        fields_dict = row

                    # Extract and parse the timestamp
                    try:
                        timestamp = parse_timestamp(row["time"])
                        bucket_start = timestamp - (timestamp - datetime.datetime.min) % time_bucket_size
                    except Exception as e:
                        influxdb3_local.error(f"Error parsing timestamp: {e}, Time value: {row['time']}, Type: {type(row['time'])}")
                        continue

                    for field_name, field_value in fields_dict.items():
                        try:
                            field_value = float(field_value)
                        except:
                            continue

                        # Store values for median, mode, and 95th percentile
                        time_buckets[bucket_start]["values"][field_name].append(field_value)

                        # Update min, max, sum, and count for the time bucket
                        if field_name not in time_buckets[bucket_start]["stats"]:
                            time_buckets[bucket_start]["stats"][field_name] = {
                                "min": field_value,
                                "max": field_value,
                                "sum": field_value,
                                "count": 1,
                            }
                        else:
                            bucket_stats = time_buckets[bucket_start]["stats"][field_name]
                            bucket_stats["min"] = min(bucket_stats["min"], field_value)
                            bucket_stats["max"] = max(bucket_stats["max"], field_value)
                            bucket_stats["sum"] += field_value
                            bucket_stats["count"] += 1

                # Process each time bucket
                for bucket_start, bucket_data in time_buckets.items():
                    for field_name, values in bucket_data["values"].items():
                        bucket_stats = bucket_data["stats"][field_name]

                        min_value = bucket_stats["min"]
                        max_value = bucket_stats["max"]
                        mean_value = bucket_stats["sum"] / bucket_stats["count"]
                        median_value = calculate_median(values)
                        mode_value = calculate_mode(values)
                        percentile_95 = calculate_percentile(values, 95)

                        analytics_line = LineBuilder(analytics_table)\
                            .tag("field_name", field_name)\
                            .tag("time_bucket", bucket_start.isoformat())\
                            .float64_field("min", min_value)\
                            .float64_field("max", max_value)\
                            .float64_field("mean", mean_value)\
                            .float64_field("median", median_value)\
                            .float64_field("mode", mode_value if mode_value is not None else 0)\
                            .float64_field("95Percentile", percentile_95)\
                            .float64_field("count", bucket_stats["count"])

                        influxdb3_local.write(analytics_line)

            else:
                # Process without time buckets (original logic)
                for row in sorted_rows:  # Process rows in chronological order
                    if "fields" in row:
                        fields_dict = row["fields"]
                    else:
                        fields_dict = row

                    for field_name, field_value in fields_dict.items():
                        try:
                            field_value = float(field_value)
                        except:
                            continue

                        # Store values for median, mode, and 95th percentile
                        if field_name not in field_values_list:
                            field_values_list[field_name] = []
                        field_values_list[field_name].append(field_value)

                        new_stats["count"][field_name] = new_stats["count"].get(field_name, 0) + 1
                        new_stats["sum"][field_name] = new_stats["sum"].get(field_name, 0) + field_value

                        if field_name not in new_stats["min"] or field_value < new_stats["min"][field_name]:
                            new_stats["min"][field_name] = field_value

                        if field_name not in new_stats["max"] or field_value > new_stats["max"][field_name]:
                            new_stats["max"][field_name] = field_value

                if has_existing_stats:
                    influxdb3_local.info("Table exists!")
                    for field_name in new_stats["count"].keys():
                        total_count = current_stats.get("count", {}).get(field_name, 0) + new_stats["count"][field_name]
                        total_sum = current_stats.get("sum", {}).get(field_name, 0) + new_stats["sum"][field_name]

                        min_value = min(
                            current_stats.get("min", {}).get(field_name, float("inf")),
                            new_stats["min"].get(field_name, float('inf'))
                        )

                        max_value = max(
                            current_stats.get("max", {}).get(field_name, float('-inf')),
                            new_stats["max"].get(field_name, float('-inf'))
                        )

                        mean_value = total_sum / total_count if total_count > 0 else 0
                        median_value = calculate_median(field_values_list[field_name])
                        mode_value = calculate_mode(field_values_list[field_name])
                        percentile_95 = calculate_percentile(field_values_list[field_name], 95)

                        analytics_line = LineBuilder(analytics_table)\
                            .tag("field_name", field_name)\
                            .float64_field("min", min_value)\
                            .float64_field("max", max_value)\
                            .float64_field("mean", mean_value)\
                            .float64_field("median", median_value)\
                            .float64_field("mode", mode_value if mode_value is not None else 0)\
                            .float64_field("95Percentile", percentile_95)\
                            .float64_field("count", total_count)

                        influxdb3_local.write(analytics_line)

                else:
                    influxdb3_local.info("Table does not exist!")
                    for field_name in new_stats["count"].keys():
                        count = new_stats["count"][field_name]
                        min_value = new_stats["min"][field_name]
                        max_value = new_stats["max"][field_name]
                        mean_value = new_stats["sum"][field_name] / count if count > 0 else 0
                        median_value = calculate_median(field_values_list[field_name])
                        mode_value = calculate_mode(field_values_list[field_name])
                        percentile_95 = calculate_percentile(field_values_list[field_name], 95)

                        analytics_line = LineBuilder(analytics_table)\
                            .tag("field_name", field_name)\
                            .float64_field("min", min_value)\
                            .float64_field("max", max_value)\
                            .float64_field("mean", mean_value)\
                            .float64_field("median", median_value)\
                            .float64_field("mode", mode_value if mode_value is not None else 0)\
                            .float64_field("95th_percentile", percentile_95)\
                            .float64_field("count", count)

                        influxdb3_local.write(analytics_line)

    influxdb3_local.info("Analytics data collected with median, mode, and 95th percentile!")