"""Plugin entry point for the `process_request` trigger."""


def process_request(influxdb3_local, query_params, request_headers, request_body, args=None):
    """Echo request inputs as a JSON response with a greeting."""
    return {
        "query_params": dict(query_params),
        "request_headers": dict(request_headers),
        "request_body": request_body.decode("utf-8", errors="replace"),
    }
