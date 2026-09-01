"""
{
    "plugin_type": ["scheduled", "http"],
    "scheduled_args_config": [
        {"name": "measurement", "example": "sensors", "description": "Source measurement (table) to read from.", "required": true},
        {"name": "field", "example": "pressure", "description": "The numeric field to predict (the regression target y). The plugin trains on rows where this field is present and predicts the rows where it is null.", "required": true},
        {"name": "feature_fields", "example": "temp humidity", "description": "Numeric feature columns (X) used to predict `field`, separated by spaces (e.g. 'temp humidity'). Use spaces, not commas: --trigger-arguments splits argument pairs on commas. A field name containing a space is only reachable via a TOML config file or the HTTP JSON body, where this may be a list.", "required": true},
        {"name": "window", "example": "30d", "description": "Time window of rows to read from InfluxDB, ending at the trigger's call time. Units: s,min,h,d,w (integer magnitude).", "required": false},
        {"name": "start_time", "example": "2026-01-01T00:00:00Z", "description": "ISO start of a fixed window instead of a trailing one. With skip_existing left on, a schedule over a fixed window backfills and then stops calling the gateway.", "required": false},
        {"name": "end_time", "example": "2026-02-01T00:00:00Z", "description": "ISO end of a fixed window. Given alone, the window starts one `window` earlier.", "required": false},
        {"name": "tags", "example": "site:A", "description": "Filter to a single series. Format: key:val key2:val2 (space-separated pairs, single value per key). A token without ':' is rejected. Required if the window holds more than one series.", "required": false},
        {"name": "model", "example": "synthefy/nori-30m", "description": "The Nori gateway slug to call. Required: there is no default, because the slug selects a priced model. The current list of models and their slugs is at https://docs.synthefy.com/nori/quickstart#models. The bare 'synthefy/nori' slug is retired. Your API key must be granted the slug.", "required": true},
        {"name": "output_measurement", "example": "sensors_regressed", "description": "Where to write predictions. Default: <measurement>_regressed.", "required": false},
        {"name": "target_database", "example": "predictions", "description": "Write predictions to this database instead of the trigger's own.", "required": false},
        {"name": "dry_run", "example": "false", "description": "If true, log predictions but do not write them.", "required": false},
        {"name": "min_history", "example": "50", "description": "Minimum labeled rows (target present) required to train; abort below this.", "required": false},
        {"name": "max_train_rows", "example": "1000", "description": "Cap on labeled rows sent as the in-context training set; the most recent rows are kept. Each row is billed by the gateway.", "required": false},
        {"name": "max_predict_rows", "example": "5000", "description": "Cap on rows predicted per run; the most recent rows are kept.", "required": false},
        {"name": "max_read_rows", "example": "50000", "description": "Ceiling on rows read from InfluxDB in one run, as a LIMIT on the query. The most recent rows are read and a truncated read is logged. Guards against a very wide window.", "required": false},
        {"name": "predict_batch_size", "example": "1000", "description": "Rows per gateway call. Every batch re-sends the training context and is billed separately, so a larger value costs less.", "required": false},
        {"name": "request_timeout", "example": "300s", "description": "Timeout for one gateway call. A cold start can take 60-130s, so keep this well above that.", "required": false},
        {"name": "max_retries", "example": "3", "description": "Maximum attempts per gateway call and per write (1 disables retry).", "required": false},
        {"name": "skip_existing", "example": "true", "description": "Skip rows that already hold a prediction in output_measurement, so a repeating schedule does not re-send and re-bill the same rows. Set false to refresh earlier predictions with newer training data.", "required": false},
        {"name": "config_file_path", "example": "nori_regression_config_scheduler.toml", "description": "Path to a TOML file supplying all parameters, relative to PLUGIN_DIR. Mutually exclusive with inline trigger arguments.", "required": false}
    ],
    "http_args_config": [
        {"name": "measurement", "example": "sensors", "description": "Source measurement. May also be provided in the JSON request body.", "required": false},
        {"name": "field", "example": "pressure", "description": "Target field to predict. May also be in the request body.", "required": false},
        {"name": "feature_fields", "example": "temp humidity", "description": "Feature columns. May also be in the request body, as a JSON list of column names or a space-separated string.", "required": false},
        {"name": "tags", "example": "site:A", "description": "Filter to a single series, as space-separated key:val pairs. May also be in the request body as a JSON object.", "required": false},
        {"name": "window", "example": "30d", "description": "Time window of rows to read, ending now. May also be in the request body. Ignored when start_time and end_time are both given.", "required": false},
        {"name": "start_time", "example": "2026-01-01T00:00:00Z", "description": "ISO start of the window. May also be in the request body. Given alone, the window ends now.", "required": false},
        {"name": "end_time", "example": "2026-02-01T00:00:00Z", "description": "ISO end of the window. May also be in the request body. Given alone, the window starts one `window` earlier.", "required": false},
        {"name": "dry_run", "example": "false", "description": "If true, log predictions but do not write them. May also be in the request body.", "required": false},
        {"name": "model", "example": "synthefy/nori-30m", "description": "The Nori gateway slug to call. Required, and a trigger argument only: it selects a billed model, so there is no default and the request body cannot override it.", "required": true},
        {"name": "output_measurement", "example": "sensors_regressed", "description": "Where to write predictions. Trigger argument only: the request body cannot override a write target.", "required": false},
        {"name": "target_database", "example": "predictions", "description": "Write predictions to this database instead of the trigger's own. Trigger argument only: the request body cannot override a write target.", "required": false},
        {"name": "min_history", "example": "50", "description": "Minimum labeled rows required to train. Trigger argument only.", "required": false},
        {"name": "max_train_rows", "example": "1000", "description": "Cap on labeled rows sent as the training set. Trigger argument only: it bounds billed rows.", "required": false},
        {"name": "max_predict_rows", "example": "5000", "description": "Cap on rows predicted per request. Trigger argument only: it bounds billed rows.", "required": false},
        {"name": "max_read_rows", "example": "50000", "description": "Ceiling on rows read from InfluxDB in one run. Trigger argument only: it bounds the work a request can ask the host to do.", "required": false},
        {"name": "predict_batch_size", "example": "1000", "description": "Rows per gateway call. Trigger argument only: it bounds billed calls.", "required": false},
        {"name": "request_timeout", "example": "300s", "description": "Timeout for one gateway call. Trigger argument only.", "required": false},
        {"name": "max_retries", "example": "3", "description": "Maximum attempts per gateway call and per write. Trigger argument only.", "required": false},
        {"name": "skip_existing", "example": "true", "description": "Skip rows that already hold a prediction. Trigger argument only.", "required": false},
        {"name": "config_file_path", "example": "nori_regression_config_scheduler.toml", "description": "Path to a TOML file supplying all parameters, relative to PLUGIN_DIR. Trigger argument only: the request body cannot name a file to read. Mutually exclusive with a request body.", "required": false}
    ]
}
"""

import json
import math
import os
import random
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import urlsplit

import requests
from influxdata_plugin_utils.config import Validator, load_plugin_config
from influxdata_plugin_utils.parsing import (
    parse_bool,
    parse_delimited_list,
    parse_key_value,
    parse_timedelta,
)
from influxdata_plugin_utils.write import write_data

# Note: LineBuilder is provided by the InfluxDB 3 plugin framework at runtime.

# --- Nori gateway ----------------------------------------------------------
# The gateway routes by the `model` slug in the request body; the OUTGOING request authenticates
# with an `Authorization: Api-Key <key>` header (see _call_nori).
#
# The endpoint is a module constant, not a parameter: it receives the operator's API key and the
# training data, so a caller must never be able to choose it. An operator running a private gateway
# can override it with the NORI_GATEWAY_URL environment variable on the InfluxDB host, which is
# operator-controlled in the same way the key is.
DEFAULT_GATEWAY_URL = "https://inference.baseten.co/predict"
GATEWAY_URL_ENV_VAR = "NORI_GATEWAY_URL"

# The key is a SECRET: it is read from the SYNTHEFY_NORI_API_KEY environment variable on the
# InfluxDB host, or (HTTP trigger only) from an incoming X-Nori-Api-Key request header. It is NEVER
# read from trigger args or the request body (both are logged), and never from the incoming
# `Authorization` header (InfluxDB consumes that header for its own request authorization).
#
# The name carries the vendor prefix because an InfluxDB host runs plugins from several authors:
# an unprefixed NORI_API_KEY says nothing about whose service it authenticates against.
API_KEY_ENV_VAR = "SYNTHEFY_NORI_API_KEY"
API_KEY_HEADER = "X-Nori-Api-Key"

# Slugs name their parameter count so the model behind a slug never silently changes. The bare
# `synthefy/nori` slug was retired and now returns 404, so it is rejected with a pointed message.
# There is no default model. Synthefy's own client and local package both require an explicit
# size and raise rather than pick one, because a variant is a priced choice: selecting one on the
# operator's behalf spends their money and pins them to a slug they never named. This plugin
# follows that. The slug below is only the example shown in help text and suggested when a retired
# slug is used.
EXAMPLE_MODEL_SLUG = "synthefy/nori-30m"
RETIRED_MODEL_SLUGS = {"synthefy/nori"}

# Which models exist, and their sizes, is Synthefy's to publish and changes when they release one.
# This plugin names only its default and points at the vendor's list, so a new variant does not
# make the plugin's documentation wrong.
MODEL_LIST_URL = "https://docs.synthefy.com/nori/quickstart#models"

# Gateway faults worth another attempt: 408/409/425 (transient conflicts), 429 (per-key rate limit,
# 50/min) and every 5xx (a cold start can 503, or 500 once). A 4xx outside that set is a permanent
# input, key or slug problem, so retrying it only wastes the caller's time and the gateway's quota.
RETRYABLE_STATUS = frozenset({408, 409, 425, 429}) | frozenset(range(500, 600))
MAX_BACKOFF_SECONDS = 30.0

# Request bodies reach this plugin from anyone holding a database token, so only these keys may be
# taken from the body. Everything else (the model slug, the write target, the row caps, the TOML
# path) stays under the operator's control via trigger arguments or the TOML file.
BODY_OVERRIDABLE_KEYS = frozenset(
    {
        "measurement",
        "field",
        "feature_fields",
        "tags",
        "window",
        "start_time",
        "end_time",
        "dry_run",
    }
)

MAX_BODY_BYTES = 10 * 1024 * 1024

# dynaconf (under influxdata_plugin_utils.config) evaluates converter tokens on any string value
# that STARTS WITH "@": @format and @jinja interpolate `env`, @read_file reads the host filesystem,
# @get reads other settings. Every one of dynaconf's ~30 tokens begins with this single character,
# and nothing else triggers them (a leading space or a doubled @ is inert), so refusing a leading
# "@" is a complete guard. Without it a request body of
# {"measurement": "@format {env[SYNTHEFY_NORI_API_KEY]}"} comes back resolved in the error
# message - the plugin's own secret, handed to the caller.
DYNACONF_TOKEN_PREFIX = "@"

# Parameters an earlier revision accepted. They are rejected by name rather than ignored, so an
# operator following the old documentation gets a message instead of silence.
REMOVED_PARAMETERS = {
    "gateway_url": (
        "the endpoint is no longer a parameter, because the request carries the Nori API key. "
        f"Set the {GATEWAY_URL_ENV_VAR} environment variable on the InfluxDB host instead."
    ),
    "mode": "this plugin only does regression; the forecast mode was removed.",
    "horizon": "forecasting was removed; this plugin imputes rows where the target is null.",
    "step": "forecasting was removed; this plugin imputes rows where the target is null.",
    "lags": "forecasting was removed; Nori sees only the columns named in feature_fields.",
    "rolling": "forecasting was removed; Nori sees only the columns named in feature_fields.",
    "tz": "no time-derived features are computed, so no timezone is needed.",
}

# Tags every output point carries so a prediction can be traced to what produced it.
PROVENANCE_TAGS = frozenset({"model", "source", "target"})

# information_schema data types (mirror influxdata_plugin_utils.introspection).
TAG_DATA_TYPE = "Dictionary(Int32, Utf8)"
NUMERIC_TYPES = frozenset({"Int64", "UInt64", "Float64", "Int32", "Float32"})


class PublicError(Exception):
    """An error whose message is written for the caller and is safe to return over HTTP.

    `detail` carries anything that must NOT be returned - a host file path, a parser's internal
    text, a body echoed by a private gateway. The entry points log `log_text()` and return only
    `str(e)`.
    """

    def __init__(self, message: str, detail: str = ""):
        super().__init__(message)
        self.detail = detail

    def log_text(self) -> str:
        return f"{self}" + (f" [detail: {self.detail}]" if self.detail else "")


def _log_text(e: Exception) -> str:
    """The full text for the log: a PublicError's detail included, anything else as-is."""
    return e.log_text() if isinstance(e, PublicError) else str(e)


class ConfigError(PublicError):
    """A problem with the configuration, named so the caller can fix their own input."""


class GatewayError(PublicError):
    """A Nori gateway fault. The message names the status and the model slug, never the endpoint or
    the gateway's echoed body."""


# --- Configuration ---------------------------------------------------------

VALIDATORS: list = [
    # No must_exist: a missing required value is reported by _normalize_config, which names the
    # parameter and what it is for instead of dynaconf's "... is required in env main".
    Validator("measurement", default="", cast=str),
    Validator("field", default="", cast=str),
    Validator("feature_fields", default="", cast=parse_delimited_list),
    Validator("window", default="30d", cast=parse_timedelta),
    Validator("model", default="", cast=str),
    Validator("dry_run", default=False, cast=parse_bool),
    Validator("skip_existing", default=True, cast=parse_bool),
    Validator("min_history", default=50, gte=1, cast=int),
    Validator("max_train_rows", default=1000, gte=1, cast=int),
    Validator("max_predict_rows", default=5000, gte=1, cast=int),
    Validator("max_read_rows", default=50_000, gte=1, cast=int),
    Validator("predict_batch_size", default=1000, gte=1, cast=int),
    Validator("max_retries", default=3, gte=1, cast=int),
    Validator("request_timeout", default="300s", cast=parse_timedelta),
]


def _reject_dynaconf_tokens(values, where: str, path: str = "") -> None:
    """Refuse a value that dynaconf would evaluate as a converter token.

    Applied to trigger arguments and to request-body values before they reach load_plugin_config.
    A TOML file is not checked: it is authored by the operator on the host, at the same trust level
    as the environment those tokens would read.
    """
    if isinstance(values, dict):
        for key, value in values.items():
            _reject_dynaconf_tokens(value, where, f"{path}.{key}" if path else str(key))
        return
    if isinstance(values, (list, tuple)):
        for i, value in enumerate(values):
            _reject_dynaconf_tokens(value, where, f"{path}[{i}]")
        return
    if isinstance(values, str) and values.startswith(DYNACONF_TOKEN_PREFIX):
        raise ConfigError(
            f"{where} value for {path!r} may not begin with "
            f"{DYNACONF_TOKEN_PREFIX!r}: the configuration loader would evaluate it as a "
            f"substitution token and read the InfluxDB host's environment or filesystem."
        )


def _load_config(args: dict | None, body: dict | None = None) -> dict:
    """Build the typed config from trigger args, an optional TOML file and an optional HTTP body.

    A TOML file (`config_file_path`, trigger arguments only) supplies every parameter and is
    mutually exclusive with inline arguments and with a request body, matching the other plugins in
    this repository. Body keys are restricted to BODY_OVERRIDABLE_KEYS; any other key is rejected
    by name rather than dropped in silence.
    """
    args = dict(args or {})
    body = dict(body or {})

    _reject_dynaconf_tokens(args, "trigger argument")
    _reject_dynaconf_tokens(body, "request body")

    removed = sorted(set(REMOVED_PARAMETERS) & {*args, *body})
    if removed:
        raise ConfigError(
            "; ".join(f"`{key}` is not a parameter: {REMOVED_PARAMETERS[key]}" for key in removed)
        )

    if args.get("config_file_path"):
        inline = sorted(set(args) - {"config_file_path"})
        if inline:
            raise ConfigError(
                f"config_file_path supplies every parameter, so it cannot be combined with the "
                f"inline trigger argument(s) {inline}. Move them into the TOML file, or drop "
                f"config_file_path."
            )
        if body:
            raise ConfigError(
                "config_file_path supplies every parameter, so the request body must be empty. "
                "Remove config_file_path from the trigger arguments to configure per request."
            )

    if body:
        rejected = sorted(k for k in body if k not in BODY_OVERRIDABLE_KEYS)
        if rejected:
            raise ConfigError(
                f"request body may not set {rejected}: these are operator settings. "
                f"Set them as trigger arguments (or in the TOML config file) instead. "
                f"Body-overridable keys: {sorted(BODY_OVERRIDABLE_KEYS)}."
            )
        # A trigger argument PINS its value: the body may fill in what the operator left open, but
        # not move what the operator chose. Otherwise a body-settable `measurement` re-points the
        # read, and with it the `<measurement>_regressed` write target the operator expected.
        pinned = sorted(k for k in body if k in args)
        if pinned:
            raise ConfigError(
                f"request body may not override {pinned}: the trigger already fixes "
                f"{'them' if len(pinned) > 1 else 'it'}. Create a trigger without "
                f"{'those arguments' if len(pinned) > 1 else 'that argument'} to set "
                f"{'them' if len(pinned) > 1 else 'it'} per request."
            )
        # An explicit JSON null means "unset": drop those keys so the validator default applies.
        args.update({k: v for k, v in body.items() if v is not None})

    toml_path = args.get("config_file_path")
    try:
        settings = load_plugin_config(
            args, validators=VALIDATORS, source="toml" if toml_path else "args"
        )
    except Exception as e:
        if toml_path:
            # The resolved path and the parser's text are operator-side detail, and an HTTP caller
            # reaches this by posting an empty body to a TOML-configured trigger.
            raise ConfigError(
                "the trigger's TOML configuration could not be loaded; see the plugin logs for "
                "this task_id",
                detail=f"{toml_path}: {type(e).__name__}: {e}",
            ) from e
        raise ConfigError(f"invalid configuration: {e}") from e

    return _normalize_config(settings)


def _normalize_config(cfg) -> dict:
    """Validate cross-field constraints and return a plain dict."""
    measurement = str(cfg.get("measurement") or "").strip()
    field = str(cfg.get("field") or "").strip()
    if not measurement:
        raise ConfigError("`measurement` is required: the source measurement (table) to read")
    if not field:
        raise ConfigError("`field` is required: the numeric field to predict")

    # dict.fromkeys de-duplicates while preserving order: a repeated column would otherwise
    # produce a duplicate SQL projection and a duplicated feature in every row.
    feature_fields = list(dict.fromkeys(cfg.get("feature_fields") or []))
    if not feature_fields:
        raise ConfigError(
            "`feature_fields` is required: the space-separated numeric feature columns (X) "
            "used to predict `field`"
        )
    reserved = [f for f in feature_fields if f == field or f == "time"]
    if reserved:
        raise ConfigError(
            f"feature_fields cannot include the target field or 'time': {reserved}"
        )

    model = str(cfg.get("model") or "").strip()
    if not model:
        raise ConfigError(
            "`model` is required: the Nori gateway slug to call, for example "
            f"{EXAMPLE_MODEL_SLUG!r}. There is no default, because the slug selects a priced "
            f"model. The current slugs are listed at {MODEL_LIST_URL}."
        )
    if model in RETIRED_MODEL_SLUGS:
        raise ConfigError(
            f"model slug {model!r} is retired and no longer routes. Use {EXAMPLE_MODEL_SLUG!r} "
            f"or another current slug, listed at {MODEL_LIST_URL}."
        )

    try:
        tags = parse_key_value(cfg.get("tags") or {}, pair_sep=" ", kv_sep=":")
    except ValueError as e:
        raise ConfigError(
            f"invalid `tags`: {e}. Format: 'key:val key2:val2' (space-separated pairs)."
        ) from e

    request_timeout = cfg.request_timeout.total_seconds()
    if request_timeout <= 0:
        raise ConfigError("`request_timeout` must be positive")

    if cfg.min_history > cfg.max_train_rows:
        raise ConfigError(
            f"min_history ({cfg.min_history}) exceeds max_train_rows ({cfg.max_train_rows}), "
            f"so no run can ever qualify"
        )

    output_measurement = str(cfg.get("output_measurement") or "").strip() or (
        f"{measurement}_regressed"
    )
    if output_measurement == measurement:
        raise ConfigError(
            "output_measurement must differ from measurement: the plugin would otherwise write "
            "predictions into the column it reads as ground truth"
        )

    return {
        "measurement": measurement,
        "field": field,
        "feature_fields": feature_fields,
        "tags": tags,
        "window": cfg.window,
        "start_time": str(cfg.get("start_time") or "").strip() or None,
        "end_time": str(cfg.get("end_time") or "").strip() or None,
        "model": model,
        "output_measurement": output_measurement,
        "target_database": str(cfg.get("target_database") or "").strip() or None,
        "dry_run": cfg.dry_run,
        "skip_existing": cfg.skip_existing,
        "min_history": cfg.min_history,
        "max_train_rows": cfg.max_train_rows,
        "max_predict_rows": cfg.max_predict_rows,
        "max_read_rows": cfg.max_read_rows,
        "predict_batch_size": cfg.predict_batch_size,
        "max_retries": cfg.max_retries,
        "request_timeout": request_timeout,
    }


# --- Schema and window -----------------------------------------------------


def _ident(name) -> str:
    """Quote a SQL identifier, escaping embedded double-quotes.

    Identifiers (measurement / field / tag-key names) cannot be passed as query parameters, so they
    are interpolated, but quoted and escaped so a name containing a quote can neither break the
    query nor inject SQL. Tag and time *values* are passed as bound parameters (see _build_where).
    """
    return '"' + str(name).replace('"', '""') + '"'


def _resolve_schema(influxdb3_local, cfg) -> dict:
    """Read the measurement's columns and types, and reject a configuration the schema cannot serve.

    A column-name check alone is not enough: a tag or string column named as a feature would pass
    it, then coerce to null for every row, and the user would see 'only 0 labeled rows' instead of
    the real fault. So the target and every feature must be a numeric column here.
    """
    rows = influxdb3_local.query(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $m",
        {"m": cfg["measurement"]},
    )
    columns = {r["column_name"]: r.get("data_type", "") for r in (rows or [])}
    if not columns:
        raise ConfigError(
            f"measurement {cfg['measurement']!r} not found (no columns). Check the name and that "
            f"the trigger runs against the database holding it."
        )

    tag_names = sorted(k for k, t in columns.items() if t == TAG_DATA_TYPE)

    def _require_numeric(name: str, role: str) -> None:
        if name not in columns:
            raise ConfigError(
                f"{role} {name!r} not found in {cfg['measurement']!r}. Available columns: "
                f"{sorted(columns)}"
            )
        if columns[name] not in NUMERIC_TYPES:
            raise ConfigError(
                f"{role} {name!r} is {columns[name]}, not a numeric field. Nori regresses on "
                f"numeric columns only."
            )

    _require_numeric(cfg["field"], "target field")
    for feature in cfg["feature_fields"]:
        _require_numeric(feature, "feature field")

    missing_tags = [k for k in cfg["tags"] if k not in columns]
    if missing_tags:
        raise ConfigError(
            f"tag column(s) {missing_tags} not found in {cfg['measurement']!r}. Tag columns: "
            f"{tag_names}"
        )
    non_tags = [k for k in cfg["tags"] if k in columns and columns[k] != TAG_DATA_TYPE]
    if non_tags:
        raise ConfigError(
            f"`tags` filter names field column(s) {non_tags}, not tags. Tag columns: {tag_names}"
        )

    # Output points carry provenance tags named model/source/target. A source tag with one of those
    # names would overwrite the provenance on write, and would also make the skip_existing lookup
    # self-contradictory ("source" = <a tag value> AND "source" = <the measurement>), so the run
    # would never skip and would re-pay for the same rows forever. Refuse up front.
    clashing = sorted(set(tag_names) & PROVENANCE_TAGS)
    if clashing:
        raise ConfigError(
            f"{cfg['measurement']!r} has tag column(s) {clashing} whose names collide with the "
            f"provenance tags this plugin writes ({sorted(PROVENANCE_TAGS)}). Set "
            f"`output_measurement` on a measurement without that clash, or rename the source tag."
        )

    return {"columns": columns, "tag_names": tag_names}


def _rfc3339(dt: datetime) -> str:
    # A naive value is UTC, never the host's local time: otherwise a query bound would silently
    # depend on the server's timezone.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(name: str, raw: str) -> datetime:
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError as e:
        raise ConfigError(
            f"`{name}` must be an ISO 8601 datetime (e.g. 2026-01-01T00:00:00Z), got {raw!r}"
        ) from e
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _resolve_window(cfg, now: datetime) -> tuple[datetime, datetime]:
    """Resolve the read window, honouring each bound on its own.

    Either bound may be given alone: `start_time` alone reads up to `now`, `end_time` alone reads
    the `window` before it. Only when neither is given does the window hang off `now`. An earlier
    version fell back to the full default window whenever one bound was missing, which silently
    read a different range than the caller asked for.
    """
    start_raw, end_raw = cfg["start_time"], cfg["end_time"]
    if start_raw and end_raw:
        start, end = _parse_iso("start_time", start_raw), _parse_iso("end_time", end_raw)
    elif start_raw:
        start, end = _parse_iso("start_time", start_raw), now
    elif end_raw:
        end = _parse_iso("end_time", end_raw)
        start = end - cfg["window"]
    else:
        end, start = now, now - cfg["window"]

    if start >= end:
        raise ConfigError(
            f"empty time window: start {_rfc3339(start)} is not before end {_rfc3339(end)}"
        )
    return start, end


# --- Reading ---------------------------------------------------------------


def _build_where(tags: dict, start: datetime, end: datetime) -> tuple[str, dict]:
    """Build the WHERE clause with bound parameters for the window and every tag value.

    Tag and time *values* are bound parameters, never string-concatenated, and identifiers are
    quote-escaped, so a quote in a value or a column name can neither break nor inject the query.
    """
    params: dict = {"start_ts": _rfc3339(start), "end_ts": _rfc3339(end)}
    clause = "time >= $start_ts AND time < $end_ts"
    for i, (key, value) in enumerate(tags.items()):
        name = f"tag{i}"
        clause += f" AND {_ident(key)} = ${name}"
        params[name] = value
    return clause, params


def _to_float(value):
    """Coerce a queried value to a finite float, or None. Booleans are not numbers here."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _read_rows(influxdb3_local, cfg, schema, start, end, task_id) -> list[dict]:
    """Read (time, target, features, tags) for the window.

    Every tag column is selected, not only the filtered ones: the series identity decides whether
    the run is single-series, and the output points carry it.
    """
    features = cfg["feature_fields"]
    tag_names = schema["tag_names"]
    where, params = _build_where(cfg["tags"], start, end)
    # No `AS y` alias: a source tag column literally named `y` would collide with it and make the
    # query unplannable. Columns are read back under their own names.
    selected = ", ".join(
        ["time", _ident(cfg["field"]), *(_ident(f) for f in features), *(_ident(t) for t in tag_names)]
    )
    # DESC + LIMIT, reversed below: the row caps alone bound what is SENT to the gateway, but not
    # what is READ. `window` is settable from the HTTP body, so without a ceiling here a caller
    # could make the plugin pull a whole retention period into this process. The ceiling is well
    # above any normal run, and a truncated read warns rather than passing silently.
    limit = cfg["max_read_rows"]
    sql = (
        f"SELECT {selected} FROM {_ident(cfg['measurement'])} "
        f"WHERE {where} ORDER BY time DESC LIMIT {int(limit)}"
    )
    influxdb3_local.info(
        f"[{task_id}] reading {cfg['measurement']}.{cfg['field']} from "
        f"{params['start_ts']} to {params['end_ts']}"
    )
    rows = influxdb3_local.query(sql, params) or []
    if len(rows) >= limit:
        influxdb3_local.warn(
            f"[{task_id}] the window holds at least max_read_rows ({limit}) rows; reading only the "
            f"most recent {limit}. Narrow `window` (or raise max_read_rows) so the whole range is "
            f"considered."
        )

    parsed: list[dict] = []
    for row in rows:
        if row.get("time") is None:
            continue
        values = [_to_float(row.get(f)) for f in features]
        if any(v is None for v in values):
            # A row needs every feature to train on or to be predicted.
            continue
        parsed.append(
            {
                "time_ns": int(row["time"]),
                "y": _to_float(row.get(cfg["field"])),
                "x": values,
                "series": tuple(
                    (t, str(row[t])) for t in tag_names if row.get(t) not in (None, "")
                ),
            }
        )
    # Sorted here, not left to the query's DESC ordering: everything downstream reads "the most
    # recent rows" as a suffix, so the order is this function's guarantee rather than the engine's.
    parsed.sort(key=lambda r: r["time_ns"])
    return parsed


def _existing_prediction_times(influxdb3_local, cfg, series_tags, start, end, task_id) -> set:
    """Timestamps in the window that already hold a prediction from this plugin, for this series.

    Without this, every scheduled tick re-sends a byte-identical payload to a metered endpoint:
    predictions land in a separate measurement, so the source rows stay null and nothing converges.
    At `every:15m` over a 30d window that is roughly 2,880 paid sends of each row.

    The source series tags are part of the filter, so a sibling trigger writing another series into
    the same output measurement cannot make this run believe its own rows are already done.
    """
    where, params = _build_where(series_tags, start, end)
    params["src"] = cfg["measurement"]
    params["tgt"] = cfg["field"]
    sql = (
        f"SELECT time FROM {_ident(cfg['output_measurement'])} "
        f"WHERE {where} AND \"source\" = $src AND \"target\" = $tgt"
    )
    try:
        if cfg["target_database"]:
            rows = influxdb3_local.query(sql, params, database=cfg["target_database"])
        else:
            rows = influxdb3_local.query(sql, params)
    except Exception as e:  # noqa: BLE001 - a missing output measurement is the normal first run
        # Warn, not info: on the first run this is expected, but any other failure here means the
        # run is about to re-send rows it may already have paid for, and that deserves attention.
        influxdb3_local.warn(
            f"[{task_id}] could not read existing predictions from {cfg['output_measurement']} "
            f"({type(e).__name__}: {e}); treating the whole window as unpredicted, so rows that "
            f"already have a prediction may be sent again"
        )
        return set()
    return {int(r["time"]) for r in (rows or []) if r.get("time") is not None}


# --- Gateway ---------------------------------------------------------------


def _gateway_url() -> str:
    """The gateway endpoint: the module constant, or an operator-set environment override.

    HTTPS is required because the request carries the operator's API key. Loopback is allowed over
    plain HTTP so a local mock gateway can be used in tests.
    """
    url = os.environ.get(GATEWAY_URL_ENV_VAR, "").strip() or DEFAULT_GATEWAY_URL
    # urlsplit, not string slicing: .hostname drops any userinfo, so a URL like
    # "http://localhost@evil.example/" resolves to evil.example and is rejected.
    try:
        parts = urlsplit(url)
    except ValueError as e:
        # urlsplit itself rejects some malformed authorities (a bracketed host that is not an IP).
        # Report that as the configuration error it is, not as an unexplained internal failure.
        raise ConfigError(f"{GATEWAY_URL_ENV_VAR} is not a valid URL", detail=str(e)) from e
    if parts.scheme == "https" and parts.hostname:
        return url
    if parts.scheme == "http" and parts.hostname in ("localhost", "127.0.0.1", "::1"):
        return url
    raise ConfigError(
        f"{GATEWAY_URL_ENV_VAR} must be an https:// URL with a host (the request carries the Nori "
        f"API key); plain http:// is allowed only for localhost"
    )


def _get_api_key(request_headers=None) -> str:
    """Resolve the gateway key: a non-empty X-Nori-Api-Key header wins (HTTP trigger), else the
    SYNTHEFY_NORI_API_KEY environment variable.

    An empty or non-string header value falls through to the environment rather than winning it: a
    header sent with no value used to suppress the fallback and send `Api-Key ` with no key at all.

    The key is deliberately NOT read from the incoming `Authorization` header: InfluxDB parses that
    header for its own request authorization, so a custom scheme there never reaches the plugin.
    """
    if request_headers:
        for key, value in request_headers.items():
            if not (isinstance(key, str) and key.lower() == API_KEY_HEADER.lower()):
                continue
            if isinstance(value, (list, tuple)):  # some servers deliver repeated headers as a list
                value = value[0] if value else ""
            if isinstance(value, str) and value.strip():
                return value.strip()
    key = os.environ.get(API_KEY_ENV_VAR, "").strip()
    if not key:
        raise ConfigError(
            f"No Nori API key. Set the {API_KEY_ENV_VAR} environment variable on the InfluxDB "
            f"host, or pass a '{API_KEY_HEADER}: <key>' header (HTTP trigger)."
        )
    return key


def _gateway_message(resp) -> str:
    """Extract the gateway's own error text. The model returns {"detail": ...}, the gateway wraps
    errors as {"error": ...}, and a proxy in front of a cold model can return HTML."""
    try:
        payload = resp.json()
    except ValueError:
        return (resp.text or "")[:200].strip() or "(empty body)"
    if isinstance(payload, dict):
        for key in ("detail", "error", "message"):
            if payload.get(key):
                return str(payload[key])[:300]
    return str(payload)[:300]


def _validate_predictions(preds, expected: int) -> list:
    """Check the gateway's predictions before anything downstream trusts them.

    The gateway emits JSON `null` for a row whose prediction is not finite, so a null is a per-row
    outcome and is skipped. Anything else non-numeric (a NaN literal, a numeric string) breaks the
    documented contract and fails the batch: it would otherwise reach the line-protocol writer and
    fail there with an error that never names the gateway. An all-null batch fails too, so a total
    upstream failure cannot report success with nothing written.
    """
    if not isinstance(preds, list):
        raise GatewayError(
            f"gateway returned {type(preds).__name__} predictions, expected a list of {expected}"
        )
    if len(preds) != expected:
        raise GatewayError(
            f"gateway returned {len(preds)} predictions for {expected} rows"
        )
    checked: list = []
    for i, value in enumerate(preds):
        if value is None:
            checked.append(None)
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise GatewayError(
                f"prediction {i} is {type(value).__name__} ({value!r}), expected a number"
            )
        number = float(value)
        if not math.isfinite(number):
            raise GatewayError(f"prediction {i} is not finite ({value!r})")
        checked.append(number)
    if not any(v is not None for v in checked):
        raise GatewayError(
            f"every one of the {expected} predictions is null; the model produced no usable value "
            f"for this batch"
        )
    return checked


def _post_with_retry(influxdb3_local, cfg, payload, api_key, task_id):
    """POST to the gateway, retrying a transient fault.

    The local write already retries, so a bare single-attempt gateway call put the retry effort on
    the reliable side of the system: a cold model can 503, and the per-key rate limit yields 429.
    A read timeout is not retried - it has already spent the whole timeout budget, and it usually
    means the slug was never granted to this key rather than that the model is slow.
    """
    url = _gateway_url()
    headers = {"Content-Type": "application/json", "Authorization": f"Api-Key {api_key}"}
    attempts = cfg["max_retries"]
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.post(
                url, json=payload, headers=headers, timeout=cfg["request_timeout"]
            )
        except requests.exceptions.ReadTimeout as e:
            raise GatewayError(
                f"gateway did not respond within {cfg['request_timeout']:.0f}s. A cold start takes "
                f"60-130s; if this repeats, confirm the key is granted the {cfg['model']!r} slug."
            ) from e
        except requests.exceptions.RequestException as e:
            if attempt >= attempts:
                raise GatewayError(
                    f"could not reach the Nori gateway after {attempts} attempt(s): "
                    f"{type(e).__name__}"
                ) from e
            _sleep_backoff(influxdb3_local, attempt, None, task_id, type(e).__name__)
            continue

        if resp.status_code < 300:
            return resp
        detail = _gateway_message(resp)
        if resp.status_code in RETRYABLE_STATUS and attempt < attempts:
            _sleep_backoff(
                influxdb3_local, attempt, resp.headers.get("Retry-After"), task_id,
                f"HTTP {resp.status_code}",
            )
            continue
        # The status and the slug are public facts and the most useful hint a caller can get; the
        # gateway's echoed body is not, because a private NORI_GATEWAY_URL can name its own host.
        raise GatewayError(
            f"Nori gateway returned HTTP {resp.status_code} for model {cfg['model']!r}; "
            f"see the plugin logs for this task_id",
            detail=detail,
        )
    raise GatewayError("gateway retry loop exhausted")  # unreachable


def _sleep_backoff(influxdb3_local, attempt: int, retry_after, task_id: str, why: str) -> None:
    delay = (2 ** (attempt - 1)) + random.random()
    if retry_after:
        try:
            # Retry-After may also be an HTTP-date, which float() rejects; the backoff then stands.
            delay = float(retry_after)
        except (TypeError, ValueError):
            pass
    # Clamped at both ends: an absurd Retry-After must not stall the trigger, and a negative one
    # must not make time.sleep raise and abort a run that still had healthy attempts left.
    delay = max(0.0, min(delay, MAX_BACKOFF_SECONDS))
    influxdb3_local.warn(
        f"[{task_id}] gateway attempt {attempt} failed ({why}); retrying in {delay:.1f}s"
    )
    time.sleep(delay)


def _call_nori(influxdb3_local, cfg, x_train, y_train, x_test, api_key, task_id) -> list:
    """Send one in-context regression request and return the validated predictions."""
    influxdb3_local.info(
        f"[{task_id}] calling Nori: model={cfg['model']} n_features={len(x_train[0])} "
        f"n_train={len(x_train)} n_test={len(x_test)}"
    )
    payload = {
        "model": cfg["model"],
        "task": "regression",
        "X_train": x_train,
        "y_train": y_train,
        "X_test": x_test,
    }
    resp = _post_with_retry(influxdb3_local, cfg, payload, api_key, task_id)
    try:
        result = resp.json()
    except ValueError as e:
        # A proxy in front of a scaled-to-zero model can answer 200 with an HTML page. Naming the
        # gateway here keeps the fault from being reported as a bad request body.
        raise GatewayError(
            f"gateway returned a non-JSON body (HTTP {resp.status_code}, "
            f"content-type {resp.headers.get('Content-Type', 'unknown')!r})"
        ) from e
    if not isinstance(result, dict):
        raise GatewayError(f"gateway returned {type(result).__name__}, expected a JSON object")
    influxdb3_local.info(f"[{task_id}] Nori usage={result.get('usage', {})}")
    return _validate_predictions(result.get("predictions"), len(x_test))


# --- Regression ------------------------------------------------------------


def _regress(influxdb3_local, cfg, schema, start, end, api_key, task_id) -> tuple[list, list, dict, int]:
    """Predict the target from the feature columns on the same rows (tabular regression).

    Trains on the rows where the target is present and predicts the rows where it is null
    (imputation / backfill). Nori sees only the feature columns: no time features, no ordering.
    """
    rows = _read_rows(influxdb3_local, cfg, schema, start, end, task_id)
    if not rows:
        influxdb3_local.warn(
            f"[{task_id}] no rows with all of {cfg['feature_fields']} present in the window; "
            f"skipping"
        )
        return [], [], {}, 0

    # Series identity decides whether this run is single-series. Predictions are written back at
    # each row's own timestamp, so two series in one run would train as one model and write points
    # that cannot be traced to either. Counting distinct tag combinations catches that even when
    # the two series never share a timestamp, which a duplicate-timestamp check missed.
    series = {r["series"] for r in rows}
    if len(series) > 1:
        listed = ", ".join(
            "{" + ", ".join(f"{k}={v}" for k, v in s) + "}" for s in sorted(series)[:3]
        )
        raise ConfigError(
            f"the window holds {len(series)} series (e.g. {listed}), but a run must resolve to "
            f"one: predictions are written at each row's own timestamp, so several series would "
            f"train as one model and collide on write. Add a `tags` filter that isolates a single "
            f"series, or run one trigger per series."
        )
    series_tags = dict(next(iter(series)))

    train = [r for r in rows if r["y"] is not None]
    test = [r for r in rows if r["y"] is None]

    if len(train) < cfg["min_history"]:
        influxdb3_local.warn(
            f"[{task_id}] only {len(train)} labeled rows (< min_history {cfg['min_history']}); "
            f"skipping"
        )
        return [], [], series_tags, 0
    if not test:
        influxdb3_local.warn(
            f"[{task_id}] no rows to predict: every '{cfg['field']}' value in the window is "
            f"already present (this plugin fills rows where the target is null); skipping"
        )
        return [], [], series_tags, 0

    if cfg["skip_existing"]:
        done = _existing_prediction_times(
            influxdb3_local, cfg, series_tags, start, end, task_id
        )
        if done:
            before = len(test)
            test = [r for r in test if r["time_ns"] not in done]
            influxdb3_local.info(
                f"[{task_id}] skip_existing: {before - len(test)} of {before} rows already hold a "
                f"prediction in {cfg['output_measurement']}"
            )
        if not test:
            influxdb3_local.info(
                f"[{task_id}] every row in the window already holds a prediction; skipping "
                f"(set skip_existing=false to refresh them)"
            )
            return [], [], series_tags, 0

    # Caps keep one run bounded: the query has no LIMIT, so a wide window on a high-rate series
    # would otherwise build a single multi-million-row payload against the gateway's 100 MB cap.
    # The most recent rows are kept, since they are the ones a backfill cares about.
    if len(train) > cfg["max_train_rows"]:
        influxdb3_local.warn(
            f"[{task_id}] {len(train)} labeled rows exceed max_train_rows "
            f"{cfg['max_train_rows']}; training on the most recent {cfg['max_train_rows']}"
        )
        train = train[-cfg["max_train_rows"] :]
    if len(test) > cfg["max_predict_rows"]:
        influxdb3_local.warn(
            f"[{task_id}] {len(test)} rows to predict exceed max_predict_rows "
            f"{cfg['max_predict_rows']}; predicting the most recent {cfg['max_predict_rows']} "
            f"and leaving {len(test) - cfg['max_predict_rows']} for a later run"
        )
        test = test[-cfg["max_predict_rows"] :]

    x_train = [r["x"] for r in train]
    y_train = [r["y"] for r in train]

    batch_size = cfg["predict_batch_size"]
    batches = [test[i : i + batch_size] for i in range(0, len(test), batch_size)]
    if len(batches) > 1:
        influxdb3_local.info(
            f"[{task_id}] predicting {len(test)} rows in {len(batches)} batches of up to "
            f"{batch_size}; each batch re-sends the {len(x_train)}-row training context and is "
            f"billed separately"
        )

    out_times: list = []
    out_preds: list = []
    unfinished = 0
    for number, batch in enumerate(batches, 1):
        try:
            preds = _call_nori(
                influxdb3_local, cfg, x_train, y_train, [r["x"] for r in batch], api_key, task_id
            )
        except PublicError as e:
            # Batches already returned are paid for. Losing them would make the next run buy the
            # same predictions again, so keep what succeeded and report the shortfall instead.
            if not out_preds:
                raise
            unfinished = len(test) - len(out_times)
            influxdb3_local.warn(
                f"[{task_id}] batch {number} of {len(batches)} failed ({e}); keeping the "
                f"{len(out_preds)} predictions already returned and leaving {unfinished} row(s) "
                f"for a later run"
            )
            break
        out_times.extend(r["time_ns"] for r in batch)
        out_preds.extend(preds)

    return out_times, out_preds, series_tags, unfinished


# --- Writing ---------------------------------------------------------------


def _write_predictions(influxdb3_local, cfg, out_times, preds, series_tags, task_id) -> int:
    """Write the predictions with write_sync so a write error surfaces during trigger execution.

    Buffered write()/write_to_db() only queue points and flush after the trigger returns, so the
    plugin never learns whether the write succeeded. write_sync/write_sync_to_db (InfluxDB 3.8.2+)
    write immediately and raise on failure.

    The retry policy is the shared helper's, which catches every exception, so a permanent fault
    (a field-type conflict on the output measurement, say) spends the backoff before it fails.
    That cannot be narrowed by exception type from here: the engine raises a bare
    `builtins.Exception` for every write failure, transport and schema alike, with only the message
    to tell them apart (verified on 3.10.3). Classifying on message text would be worse than the
    wait, so the knob is the wait: `max_retries=1` writes once and fails fast.
    """
    builders = []
    skipped = 0
    for time_ns, value in zip(out_times, preds):
        if value is None:  # a row the model could not produce a finite value for
            skipped += 1
            continue
        line = (
            LineBuilder(cfg["output_measurement"])  # noqa: F821 - engine-provided global
            .tag("model", cfg["model"])
            .tag("source", cfg["measurement"])
            .tag("target", cfg["field"])
        )
        # The source series' own tags, not just the ones the caller filtered on, so a point can
        # always be traced back to the series it was predicted for.
        for key, tag_value in series_tags.items():
            line = line.tag(key, tag_value)
        line = line.float64_field("value", float(value))
        line = line.time_ns(int(time_ns))
        builders.append(line)
    if skipped:
        influxdb3_local.warn(
            f"[{task_id}] {skipped} row(s) had no finite prediction and were not written"
        )
    if not builders:
        return 0
    write_data(
        influxdb3_local,
        builders,
        batch=True,
        retries=cfg["max_retries"] - 1,
        no_sync=True,
        database=cfg["target_database"],
    )
    return len(builders)


# --- Run -------------------------------------------------------------------


def _run(influxdb3_local, cfg, api_key, now, task_id) -> dict:
    schema = _resolve_schema(influxdb3_local, cfg)
    start, end = _resolve_window(cfg, now)
    out_times, preds, series_tags, unfinished = _regress(
        influxdb3_local, cfg, schema, start, end, api_key, task_id
    )
    if not preds:
        return {"status": "skipped", "written": 0}
    if cfg["dry_run"]:
        influxdb3_local.info(f"[{task_id}] dry_run: first preds={preds[:5]}")
        return {"status": "dry_run", "written": 0, "predictions": preds}
    written = _write_predictions(
        influxdb3_local, cfg, out_times, preds, series_tags, task_id
    )
    influxdb3_local.info(
        f"[{task_id}] wrote {written} predictions to {cfg['output_measurement']}"
    )
    if unfinished:
        # A gateway fault stopped the run part-way. The batches that did return are written, but
        # reporting "success" would hide the shortfall from a caller who cannot read the log.
        return {"status": "partial", "written": written, "remaining": unfinished}
    return {"status": "success", "written": written}


# --- Entry points ----------------------------------------------------------


def process_scheduled_call(influxdb3_local, call_time, args=None):
    """Scheduled entry point: impute the rows still missing the target in the trailing window."""
    task_id = str(uuid.uuid4())
    try:
        cfg = _load_config(args)
        api_key = _get_api_key()  # scheduled: no request headers, so the env var only
        # The engine passes call_time as naive UTC; anchoring the window to it keeps the range
        # reproducible for a given tick.
        now = datetime.now(timezone.utc)
        if isinstance(call_time, datetime):
            now = call_time.replace(tzinfo=timezone.utc) if not call_time.tzinfo else call_time
        _run(influxdb3_local, cfg, api_key, now, task_id)
    except Exception as e:  # noqa: BLE001 - never let a scheduled run crash the engine
        influxdb3_local.error(f"[{task_id}] {type(e).__name__}: {_log_text(e)}")


def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    """HTTP entry point: impute on demand, optionally over an explicit start_time/end_time window."""
    task_id = str(uuid.uuid4())

    # Decoding the body is its own step: requests.exceptions.JSONDecodeError subclasses
    # json.JSONDecodeError, so a shared handler reported a gateway that answered with non-JSON as
    # 'invalid JSON in request body' and sent the operator to debug the wrong system.
    try:
        body = _decode_body(request_body)
    except ConfigError as e:
        influxdb3_local.error(f"[{task_id}] {e}")
        return {"status": "failed", "task_id": task_id, "message": str(e)}

    try:
        cfg = _load_config(args, body)
        api_key = _get_api_key(request_headers)
        result = _run(influxdb3_local, cfg, api_key, datetime.now(timezone.utc), task_id)
        # Surface the real outcome (success / skipped / dry_run) at the top level, not always
        # "success", so a caller can tell a real prediction from a no-op.
        return {"status": result["status"], "task_id": task_id, "result": result}
    except PublicError as e:
        # The message is written for the caller; anything that must not travel is in e.detail and
        # goes only to the log.
        influxdb3_local.error(f"[{task_id}] {type(e).__name__}: {e.log_text()}")
        return {"status": "failed", "task_id": task_id, "message": str(e)}
    except Exception as e:  # noqa: BLE001
        # Anything else can carry internal detail (a storage error, a database name), so the detail
        # goes to the log and the caller gets a stable message plus the task id to correlate on.
        influxdb3_local.error(f"[{task_id}] {type(e).__name__}: {e}")
        return {
            "status": "failed",
            "task_id": task_id,
            "message": "internal error; see the plugin logs for this task_id",
        }


def _decode_body(request_body) -> dict:
    """Decode the HTTP request body into a dict. An empty body means 'use the trigger arguments'."""
    if request_body is None or request_body == "" or request_body == b"":
        return {}
    if isinstance(request_body, dict):
        return request_body
    if not isinstance(request_body, (str, bytes, bytearray)):
        # Guarded before len(): an unexpected type would otherwise raise TypeError straight out of
        # process_request, bypassing the sanitised error contract.
        raise ConfigError(
            f"request body must be JSON text, got {type(request_body).__name__}"
        )
    if len(request_body) > MAX_BODY_BYTES:
        raise ConfigError(f"request body exceeds {MAX_BODY_BYTES // (1024 * 1024)} MiB")
    try:
        text = (
            request_body.decode("utf-8")
            if isinstance(request_body, (bytes, bytearray))
            else request_body
        )
        body = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ConfigError("request body is not valid JSON") from e
    if not isinstance(body, dict):
        raise ConfigError("request body must be a JSON object")
    return body
