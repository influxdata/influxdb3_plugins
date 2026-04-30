"""Plugin entry point for the `process_scheduled_call` trigger."""


def process_scheduled_call(influxdb3_local, schedule_time, args):
    """Called on each scheduled fire. `schedule_time` is a naive UTC datetime."""
    influxdb3_local.info(f"scheduled call at {schedule_time}")
