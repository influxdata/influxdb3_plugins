"""
{
    "plugin_type": ["scheduled", "http"],
    "scheduled_args_config": [
        {"name": "measurement", "example": "sensors", "description": "Source measurement (table) to read from.", "required": true},
        {"name": "field", "example": "pressure", "description": "The numeric field to predict (the regression target y). The plugin trains on rows where this field is present and predicts the rows where it is null.", "required": true},
        {"name": "feature_fields", "example": "temp humidity", "description": "Numeric feature columns (X) used to predict `field`, separated by spaces (e.g. 'temp humidity'). Use spaces, not commas: --trigger-arguments splits argument pairs on commas. In the HTTP JSON body this may also be a list. Required.", "required": true},
        {"name": "window", "example": "30d", "description": "Time window of rows to read from InfluxDB. Units: s,min,h,d,w.", "required": false},
        {"name": "tags", "example": "site:A", "description": "Filter to a single series. Format: key:val key2:val2 (space-separated pairs, single value per key). Required if the measurement holds more than one series.", "required": false},
        {"name": "model", "example": "synthefy/nori", "description": "The Nori gateway slug to call. Available slugs: synthefy/nori (default) and synthefy/nori-30m (a 30M-parameter variant). Your API key must be granted this slug.", "required": false},
        {"name": "gateway_url", "example": "https://inference.baseten.co/predict", "description": "Nori gateway endpoint.", "required": false},
        {"name": "output_measurement", "example": "sensors_regressed", "description": "Where to write predictions. Default: <measurement>_regressed.", "required": false},
        {"name": "target_database", "example": "predictions", "description": "Write predictions to this database instead of the trigger's own.", "required": false},
        {"name": "dry_run", "example": "false", "description": "If true, log predictions but do not write them.", "required": false},
        {"name": "min_history", "example": "50", "description": "Minimum labeled rows (target present) required to train; abort below this.", "required": false}
    ],
    "http_args_config": [
        {"name": "measurement", "example": "sensors", "description": "Source measurement. May also be provided in the JSON request body.", "required": false},
        {"name": "field", "example": "pressure", "description": "Target field to predict. May also be in the request body.", "required": false},
        {"name": "feature_fields", "example": "temp humidity", "description": "Feature columns. In the JSON request body this may be a JSON list of column names, or a space-separated string.", "required": false},
        {"name": "start_time", "example": "2026-01-01T00:00:00Z", "description": "Optional ISO start of the window (overrides `window`).", "required": false},
        {"name": "end_time", "example": "2026-02-01T00:00:00Z", "description": "Optional ISO end of the window.", "required": false}
    ]
}
"""

import json
import os
import random
import re
import time
import uuid
from datetime import timedelta

import numpy as np
import pandas as pd
import requests

# --- Nori gateway defaults -------------------------------------------------
# The gateway routes by the `model` slug in the body; the OUTGOING request to the gateway
# authenticates with an `Authorization: Api-Key <key>` header (see _call_nori). The key is a
# SECRET: the plugin reads it from the NORI_API_KEY environment variable on the InfluxDB host, or
# (HTTP trigger only) from an incoming `X-Nori-Api-Key` request header. It is NEVER read from
# trigger args or the request body (which get logged), and never from the incoming `Authorization`
# header (InfluxDB consumes that header for its own request authorization).
DEFAULT_GATEWAY_URL = "https://inference.baseten.co/predict"
DEFAULT_MODEL_SLUG = "synthefy/nori"
API_KEY_ENV_VAR = "NORI_API_KEY"

_UNIT_SECONDS = {"s": 1, "min": 60, "h": 3600, "d": 86400, "w": 604800}


def _interval_to_timedelta(text: str) -> timedelta:
    """Parse '30d' / '15min' / '1h' into a timedelta (also used to validate the `window` arg)."""
    text = str(text).strip()
    num, unit = "", ""
    for ch in text:
        (num := num + ch) if (ch.isdigit() or ch == ".") else (unit := unit + ch)
    if not num:
        raise ValueError(f"interval {text!r} has no number (expected e.g. '30d', '15min', '1h')")
    if unit not in _UNIT_SECONDS:
        raise ValueError(f"bad interval unit in {text!r} (use s/min/h/d/w)")
    return timedelta(seconds=float(num) * _UNIT_SECONDS[unit])


def _ident(name: str) -> str:
    """Quote a SQL identifier, escaping embedded double-quotes.

    Identifiers (measurement / field / tag-key names) cannot be passed as query parameters, so
    they are interpolated, but quoted and escaped so a name containing a quote can neither break
    the query nor inject SQL. Tag/time *values* are passed as bound parameters (see _where_clause).
    """
    return '"' + str(name).replace('"', '""') + '"'


def _get_api_key(request_headers=None) -> str:
    """Resolve the gateway key: the incoming `X-Nori-Api-Key` header wins (HTTP trigger), else the
    NORI_API_KEY env var.

    The key is deliberately NOT read from the incoming `Authorization` header: InfluxDB parses that
    header for its own request authorization, so a custom scheme there does not reach the plugin.
    """
    if request_headers:
        for k, v in request_headers.items():
            if k.lower() == "x-nori-api-key":  # case-insensitive per server
                return v.strip()
    key = os.environ.get(API_KEY_ENV_VAR, "").strip()
    if not key:
        raise ValueError(
            f"No Nori API key. Set the {API_KEY_ENV_VAR} env var on the InfluxDB host, "
            f"or pass an 'X-Nori-Api-Key: <key>' header (HTTP trigger)."
        )
    return key


def _split_list(text) -> list:
    """Parse a space- (or comma-) separated arg into a list of trimmed, non-empty strings.

    NOT '.'-separated: InfluxDB field names may contain dots. Prefer spaces in `--trigger-arguments`
    (the CLI splits argument pairs on commas, so a comma inside a value breaks parsing); commas are
    fine inside the HTTP JSON body.
    """
    return [x for x in re.split(r"[,\s]+", str(text).strip()) if x]


def _build_config(args: dict) -> dict:
    """Parse trigger args (strings) into a typed config with defaults."""
    args = args or {}
    measurement = args.get("measurement")
    field = args.get("field")
    tags = {}
    if args.get("tags"):
        for pair in args["tags"].split():  # space-separated key:val pairs (a value may contain '.')
            if ":" in pair:
                k, v = pair.split(":", 1)
                tags[k.strip()] = v.strip()
    return {
        "measurement": measurement,
        "field": field,
        # feature columns (X); space-separated (see _split_list). dict.fromkeys de-duplicates while
        # preserving order (a repeated column would otherwise produce a duplicate SQL projection).
        "feature_fields": list(dict.fromkeys(_split_list(args.get("feature_fields", "")))),
        "tags": tags,
        "window": args.get("window", "30d"),
        "model": args.get("model", DEFAULT_MODEL_SLUG),
        "gateway_url": args.get("gateway_url", DEFAULT_GATEWAY_URL),
        "output_measurement": args.get("output_measurement") or (f"{measurement}_regressed" if measurement else None),
        "target_database": args.get("target_database") or None,
        "dry_run": str(args.get("dry_run", "false")).lower() == "true",
        "min_history": int(args.get("min_history", 50)),
        "start_time": args.get("start_time"),
        "end_time": args.get("end_time"),
    }


def _validate_columns(influxdb3_local, cfg) -> None:
    """Fail with a clear message if the measurement / target / tag / feature columns do not exist.

    Uses a parameterized information_schema query, so a typo yields 'field X not found' instead of
    an opaque SQL error or a silent empty result.
    """
    rows = influxdb3_local.query(
        "SELECT column_name FROM information_schema.columns WHERE table_name = $m",
        {"m": cfg["measurement"]},
    )
    cols = {r.get("column_name") for r in (rows or [])}
    if not cols:
        raise ValueError(f"measurement {cfg['measurement']!r} not found (no columns)")
    if cfg["field"] not in cols:
        raise ValueError(f"target field {cfg['field']!r} not found in {cfg['measurement']!r}")
    missing = [k for k in cfg["tags"] if k not in cols]
    if missing:
        raise ValueError(f"tag column(s) {missing} not found in {cfg['measurement']!r}")
    if not cfg["feature_fields"]:
        raise ValueError("`feature_fields` is required (the '.'/','-separated feature columns X)")
    reserved = [f for f in cfg["feature_fields"] if f in ("time", "y") or f == cfg["field"]]
    if reserved:
        raise ValueError(
            f"feature_fields cannot include the target field or the reserved names time/y: {reserved}"
        )
    missing_f = [f for f in cfg["feature_fields"] if f not in cols]
    if missing_f:
        raise ValueError(f"feature field(s) {missing_f} not found in {cfg['measurement']!r}")


def _where_clause(cfg):
    """Build the WHERE clause (time window + tag filters) with bound parameters.

    Tag and time-range *values* are bound parameters (never string-concatenated) and identifiers are
    quote-escaped, so quotes in values/names can't break or inject the query. Returns
    (time_clause, tag_clause, params).
    """
    params: dict = {}
    tag_clause = ""
    for i, (k, v) in enumerate(cfg["tags"].items()):
        p = f"tag{i}"
        tag_clause += f" AND {_ident(k)} = ${p}"
        params[p] = v
    if cfg["start_time"] and cfg["end_time"]:
        time_clause = "time >= $start_ts AND time < $end_ts"
        params["start_ts"] = cfg["start_time"]
        params["end_ts"] = cfg["end_time"]
    else:
        # An INTERVAL literal can't be bound, so validate `window` is a clean interval (digits +
        # a known unit, no quotes possible) before interpolating it.
        _interval_to_timedelta(cfg["window"])
        time_clause = f"time > now() - INTERVAL '{cfg['window']}'"
    return time_clause, tag_clause, params


def _query(influxdb3_local, cfg, task_id) -> pd.DataFrame:
    """Read (time, target y, feature columns) for the window into a DataFrame.

    A row whose target is null but whose features are present is a prediction target (impute).
    Values are coerced to numeric; non-finite values become NaN so they cannot reach the gateway.
    """
    feats = cfg["feature_fields"]
    time_clause, tag_clause, params = _where_clause(cfg)
    feat_cols = ", ".join(_ident(f) for f in feats)
    sql = (
        f"SELECT time, {_ident(cfg['field'])} AS y, {feat_cols} "
        f"FROM {_ident(cfg['measurement'])} "
        f"WHERE {time_clause}{tag_clause} "
        f"ORDER BY time"
    )
    influxdb3_local.info(f"[{task_id}] query: {sql}")
    rows = influxdb3_local.query(sql, params) if params else influxdb3_local.query(sql)
    if not rows:
        return pd.DataFrame(columns=["time", "y", *feats])
    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
    df["y"] = pd.to_numeric(df["y"], errors="coerce").replace([np.inf, -np.inf], np.nan)
    for f in feats:
        df[f] = (pd.to_numeric(df[f], errors="coerce").replace([np.inf, -np.inf], np.nan)
                 if f in df.columns else np.nan)
    return df


def _call_nori(influxdb3_local, cfg, X_train, y_train, X_test, api_key, task_id):
    """Send an in-context regression request to the Nori gateway and return the predictions list.

    Raises on a transport error or an unexpected response shape.
    """
    n_features = len(X_train[0]) if X_train else 0
    influxdb3_local.info(
        f"[{task_id}] calling Nori: model={cfg['model']} "
        f"n_features={n_features} n_train={len(X_train)} n_test={len(X_test)}"
    )
    payload = {"model": cfg["model"], "task": "regression",
               "X_train": X_train, "y_train": y_train, "X_test": X_test}
    resp = requests.post(
        cfg["gateway_url"],
        json=payload,
        headers={"Content-Type": "application/json", "Authorization": f"Api-Key {api_key}"},
        timeout=120,
    )
    resp.raise_for_status()
    result = resp.json()
    preds = result.get("predictions")
    if not isinstance(preds, list) or len(preds) != len(X_test):
        raise ValueError(f"unexpected Nori response: {str(result)[:300]}")
    influxdb3_local.info(f"[{task_id}] Nori usage={result.get('usage', {})}")
    return preds


def _regress(influxdb3_local, cfg, api_key, task_id):
    """Predict the target `field` from `feature_fields` on the same rows (tabular regression).

    Trains on rows where the target is present and predicts the rows where it is null (impute /
    backfill), returning (timestamps_of_predicted_rows, predictions). No time features and no
    ordering assumptions: Nori sees only the feature columns.
    """
    feats = cfg["feature_fields"]
    df = _query(influxdb3_local, cfg, task_id)
    df = df.dropna(subset=feats)  # a row needs all features present to train on or predict
    train = df[df["y"].notna()]
    test = df[df["y"].isna()]
    if len(train) < cfg["min_history"]:
        influxdb3_local.warn(
            f"[{task_id}] only {len(train)} labeled rows (< min_history {cfg['min_history']}); skipping"
        )
        return [], []
    if len(test) == 0:
        influxdb3_local.warn(
            f"[{task_id}] no rows to predict: every '{cfg['field']}' value in the window is already "
            f"present (this plugin fills rows where the target is null); skipping"
        )
        return [], []
    # Predictions are written back at each test row's timestamp. If two rows share a timestamp
    # (a multi-series measurement not isolated by `tags`), their output points would have an
    # identical measurement+tag+time key and silently overwrite each other, so fail loud instead.
    dup = pd.Index(test["time"])[pd.Index(test["time"]).duplicated()].unique()
    if len(dup):
        raise ValueError(
            f"matched multiple series sharing {len(dup)} timestamp(s), so writing predictions "
            f"back would collide and silently drop values. Add a `tags` filter that isolates a "
            f"single series (or run one trigger per series). First colliding time: "
            f"{pd.Timestamp(dup[0])}"
        )
    X_train = train[feats].to_numpy().tolist()
    y_train = train["y"].to_numpy().tolist()
    X_test = test[feats].to_numpy().tolist()
    preds = _call_nori(influxdb3_local, cfg, X_train, y_train, X_test, api_key, task_id)
    return list(pd.DatetimeIndex(test["time"])), preds


class _BatchLines:
    """Wrap several LineBuilder objects so write_sync / write_sync_to_db can write them in one call.

    build() returns the newline-joined line protocol; each LineBuilder exposes its own build().
    """

    def __init__(self, line_builders):
        self._line_builders = list(line_builders)

    def build(self) -> str:
        lines = [str(b.build()) for b in self._line_builders]
        if not lines:
            raise ValueError("no line protocol to write")
        return "\n".join(lines)


def _write_predictions(influxdb3_local, cfg, out_times, preds, task_id, max_retries=3) -> int:
    """Write predictions with write_sync so write errors surface during trigger execution.

    Buffered write()/write_to_db() only accumulate points and flush after the trigger returns, so
    the plugin never learns whether the write succeeded. write_sync/write_sync_to_db (InfluxDB
    3.8.2+) write immediately and raise on failure; the points are batched into one line-protocol
    payload. On repeated failure this raises, so the caller reports "failed" rather than a bogus
    success count.
    """
    builders = []
    for ts, value in zip(out_times, preds):
        if value is None:
            continue
        line = (LineBuilder(cfg["output_measurement"])
                .tag("model", cfg["model"])
                .tag("source", cfg["measurement"])
                .tag("target", cfg["field"]))       # the predicted field
        for k, v in cfg["tags"].items():            # echo single-value filter tags
            line = line.tag(k, v)
        line = line.float64_field("value", float(value))
        line = line.time_ns(int(pd.Timestamp(ts).value))  # pandas ns since epoch
        builders.append(line)
    if not builders:
        return 0
    batch = _BatchLines(builders)
    db = cfg["target_database"]
    for attempt in range(max_retries):
        try:
            if db:
                influxdb3_local.write_sync_to_db(db, batch, no_sync=True)
            else:
                influxdb3_local.write_sync(batch, no_sync=True)
            return len(builders)
        except Exception as e:  # noqa: BLE001
            if attempt < max_retries - 1:
                time.sleep((2 ** attempt) + random.random())
            else:
                influxdb3_local.error(f"[{task_id}] write_sync failed after {max_retries} attempts: {e}")
                raise
    return 0


def _run(influxdb3_local, cfg, api_key, task_id) -> dict:
    if not cfg["measurement"] or not cfg["field"]:
        raise ValueError("`measurement` and `field` are required")
    _validate_columns(influxdb3_local, cfg)
    out_times, preds = _regress(influxdb3_local, cfg, api_key, task_id)
    if not preds:
        return {"status": "skipped", "written": 0}
    if cfg["dry_run"]:
        influxdb3_local.info(f"[{task_id}] dry_run: first preds={preds[:5]}")
        return {"status": "dry_run", "predictions": preds}
    written = _write_predictions(influxdb3_local, cfg, out_times, preds, task_id)
    influxdb3_local.info(f"[{task_id}] wrote {written} predictions to {cfg['output_measurement']}")
    return {"status": "success", "written": written}


# --- Entry points ----------------------------------------------------------

def process_scheduled_call(influxdb3_local, call_time, args=None):
    task_id = str(uuid.uuid4())
    try:
        cfg = _build_config(args)
        api_key = _get_api_key()  # scheduled: no request headers -> env var only
        _run(influxdb3_local, cfg, api_key, task_id)
    except Exception as e:  # noqa: BLE001 - never let a scheduled run crash the engine
        influxdb3_local.error(f"[{task_id}] {type(e).__name__}: {e}")


def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    task_id = str(uuid.uuid4())
    try:
        body = {}
        if request_body:
            body = json.loads(request_body)
        # scalar body values override trigger args
        merged = {**(args or {}), **{k: str(v) for k, v in body.items() if not isinstance(v, (list, dict))}}
        cfg = _build_config(merged)
        # complex body values are dropped by the scalar filter above, so handle them explicitly:
        if isinstance(body.get("tags"), dict):
            cfg["tags"] = {str(k): str(v) for k, v in body["tags"].items()}
        if isinstance(body.get("feature_fields"), list):
            cfg["feature_fields"] = list(dict.fromkeys(str(x) for x in body["feature_fields"]))
        api_key = _get_api_key(request_headers)
        result = _run(influxdb3_local, cfg, api_key, task_id)
        # surface the real outcome at the top level (success / skipped / dry_run), not always
        # "success", so a caller can tell a real prediction from a no-op.
        return {"status": result.get("status", "success"), "task_id": task_id, "result": result}
    except json.JSONDecodeError:
        influxdb3_local.error(f"[{task_id}] invalid JSON body")
        return {"status": "failed", "message": "invalid JSON in request body"}
    except Exception as e:  # noqa: BLE001
        influxdb3_local.error(f"[{task_id}] {type(e).__name__}: {e}")
        return {"status": "failed", "message": str(e)}
