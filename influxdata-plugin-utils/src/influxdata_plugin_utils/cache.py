"""TTL caching over the plugin runtime cache (``influxdb3_local.cache``)."""

from typing import Callable

__all__ = ["cached"]


def cached(
    influxdb3_local, key: str, producer: Callable[[], object], *, ttl_seconds: int = 3600
):
    """Return a cached value, or produce it, store it with a TTL, and return it.

    Args:
        influxdb3_local: InfluxDB client instance (exposes ``.cache``).
        key: Cache key.
        producer: Zero-arg callable that computes the value on a cache miss.
        ttl_seconds: Time-to-live for the stored value (default 1 hour).
    """
    cache = influxdb3_local.cache
    value = cache.get(key)
    if value is not None:
        return value
    value = producer()
    cache.put(key, value, ttl_seconds)
    return value