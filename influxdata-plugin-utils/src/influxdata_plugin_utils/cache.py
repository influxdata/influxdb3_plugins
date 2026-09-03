"""TTL caching over the plugin runtime cache (``influxdb3_local.cache``)."""

from typing import Callable

__all__ = ["cached"]


def cached(
    influxdb3_local,
    key: str,
    producer: Callable[[], object],
    *,
    ttl_seconds: int | None = 3600,
    refresh: bool = False,
    cache_empty: bool = True,
):
    """Return a cached value, or produce it, store it with a TTL, and return it.

    Args:
        influxdb3_local: InfluxDB client instance (exposes ``.cache``).
        key: Cache key.
        producer: Zero-arg callable that computes the value on a cache miss.
        ttl_seconds: Time-to-live for the stored value (``None`` never expires).
        refresh: Skip the lookup and replace the entry with a fresh value. When
            that value is falsy and ``cache_empty`` is off, the entry is dropped
            instead, so the next call asks again rather than reading a stale one.
        cache_empty: Store a falsy result. Set ``False`` for a lookup whose
            empty answer means "not there yet" rather than "nothing", so a
            catalog queried too early is not remembered as empty.
    """
    cache = influxdb3_local.cache
    if not refresh:
        value = cache.get(key)
        if value is not None:
            return value
    value = producer()
    if value or cache_empty:
        cache.put(key, value, ttl_seconds)
    elif refresh:
        cache.delete(key)
    return value