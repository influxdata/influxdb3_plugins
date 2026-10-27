"""
{
    "plugin_type": [
        "onwrite"
    ],
    "onwrite_args_config": [
        {
            "name": "broker_url",
            "example": "mqtt://broker.example.com:1883",
            "description": "Broker address and transport in one value: mqtt://, mqtts://, ws://, wss://, unix:///path, or a bare host[:port]. The port defaults by scheme (1883, 8883, 80, 443) and TLS is chosen by the scheme. Validated when the client is built. Required.",
            "required": true
        },
        {
            "name": "sparkplug_group_id",
            "example": "plant1",
            "description": "Sparkplug Group ID, the second token of every topic. Must not contain '/', '+' or '#'. With sparkplug_edge_node_id it is the node identity and must be unique across the infrastructure. Required.",
            "required": true
        },
        {
            "name": "sparkplug_edge_node_id",
            "example": "influxdb3",
            "description": "Sparkplug Edge Node ID, the fourth token of every topic. Must not contain '/', '+' or '#'. The group and node pair must belong to exactly one running trigger on exactly one process node. Required.",
            "required": true
        },
        {
            "name": "source_measurements",
            "example": "press_line",
            "description": "Space-separated source tables. Batches for any other table are ignored, so the trigger spec may safely be all_tables. Exactly one table is allowed when sparkplug_device_from names a tag. Required.",
            "required": true
        },
        {
            "name": "payload_fields",
            "example": "temp pressure",
            "description": "Space-separated metric columns, each optionally 'name:type' (e.g. 'count:int64') to override the datatype inferred from the schema. A datetime column may add a source time unit, 'name:datetime:unit' with unit s, ms (default), us or ns, converted to the milliseconds Sparkplug DateTime requires. This is the declared metric set of the birth certificate, and is mandatory. Required.",
            "required": true
        },
        {
            "name": "sparkplug_device_from",
            "example": "tag:machine",
            "description": "Empty (node-level NDATA, metric names 'table/field'), 'table' (per-table DDATA), or 'tag:<name>' (one device per distinct tag value, needs exactly one source table). Default empty. Republishes the birth on change.",
            "required": false
        },
        {
            "name": "client_id",
            "example": "plant-spb",
            "description": "MQTT client identifier, defaulting to influxdb3-spb-<hash of group and node> and therefore stable so the broker evicts an orphan by takeover. Set only when ACLs require a known one, and keep it stable across trigger recreations. 1-65535 UTF-8 bytes. Rebuilds the client.",
            "required": false
        },
        {
            "name": "mqtt_version",
            "example": "5",
            "description": "'3.1.1' (default) or '5'. MQTT 5 is strongly recommended: it carries the 0x8E takeover reason code and the broker-declared Maximum Packet Size, and makes orphan handover deterministic. Rebuilds the client.",
            "required": false
        },
        {
            "name": "username",
            "example": "publisher",
            "description": "User name sent in CONNECT. Never logged. Rebuilds the client.",
            "required": false
        },
        {
            "name": "password",
            "example": "<your-password>",
            "description": "Password sent in CONNECT. Never logged. Prefer the TOML file or the environment: trigger arguments are readable from the system tables. Rebuilds the client.",
            "required": false
        },
        {
            "name": "keep_alive",
            "example": "60",
            "description": "Seconds declared in CONNECT, default 60, minimum 1. A silently dropped link is detected only after about 1.5x this, and QoS 0 data sent meanwhile is lost but counted published, so 15-30 shortens that window at the cost of more PINGREQ. Rebuilds the client.",
            "required": false
        },
        {
            "name": "tls_ca_cert",
            "example": "certs/ca.pem",
            "description": "CA certificate for mqtts:// or wss://. A relative path resolves under PLUGIN_DIR; omit to use the system trust store. Rebuilds the client.",
            "required": false
        },
        {
            "name": "tls_client_cert",
            "example": "certs/client.pem",
            "description": "Client certificate for mutual TLS; set together with tls_client_key. Rebuilds the client.",
            "required": false
        },
        {
            "name": "tls_client_key",
            "example": "certs/client.key",
            "description": "Client private key for mutual TLS. Must be unencrypted; a passphrase-protected key is refused at configuration time. Rebuilds the client.",
            "required": false
        },
        {
            "name": "tls_insecure_skip_verify",
            "example": "false",
            "description": "Skip server certificate chain and hostname verification and log a warning. Default false; leave it there outside a laboratory. Rebuilds the client.",
            "required": false
        },
        {
            "name": "tls_alpn",
            "example": "x-amzn-mqtt-ca",
            "description": "ALPN protocol name, which is how AWS IoT Core serves MQTT on port 443. Default empty. Rebuilds the client.",
            "required": false
        },
        {
            "name": "ws_path",
            "example": "/mqtt",
            "description": "WebSocket endpoint path when the broker_url carries none. Default '/mqtt'. Ignored outside ws:// and wss://. Rebuilds the client.",
            "required": false
        },
        {
            "name": "ws_headers",
            "example": "Authorization:'Bearer abc' X-Tenant:'plant-1'",
            "description": "Extra headers on the WebSocket upgrade, as space-separated name:'value' pairs (or a table in TOML). Default empty. Ignored outside ws:// and wss://. Rebuilds the client.",
            "required": false
        },
        {
            "name": "reconnect_min_backoff",
            "example": "1s",
            "description": "Shortest gap between connection attempts, doubling on each failure. Duration, minimum 1s, default 1s.",
            "required": false
        },
        {
            "name": "reconnect_max_backoff",
            "example": "120s",
            "description": "Ceiling for the backoff doubling, reset once a CONNACK succeeds. Duration, at least reconnect_min_backoff, default 120s.",
            "required": false
        },
        {
            "name": "takeover_attempts",
            "example": "3",
            "description": "How many consecutive connections the broker closes within seconds of CONNACK are read as another client taking the session over, after which this one stops. Used on MQTT 3.1.1, and on MQTT 5 only when a broker DISCONNECT's reason code cannot be read. Integer >= 2, default 3. Best-effort (see README).",
            "required": false
        },
        {
            "name": "idle_disconnect",
            "example": "1h",
            "description": "After this long with no invocation, the client's threads publish NDEATH and exit, so an orphan cannot live until the server restarts. Duration, minimum 1min, default 1h. Use the all_tables spec so a sparse source table does not idle the node out.",
            "required": false
        },
        {
            "name": "filters",
            "example": "temp:'>=101' state:'!=error'",
            "description": "Scalar per-row comparisons, space-separated 'field:op value' pairs. A numeric value compares numerically; quote it to compare as text. Selects rows, not transitions. Default none.",
            "required": false
        },
        {
            "name": "allow_values",
            "example": "machine:'press7 press8'",
            "description": "Membership allow-list, space-separated tag:'v1 v2' pairs. Also the way to bound the device set and to pin a non-device tag to one value so series do not merge onto one metric. Default none.",
            "required": false
        },
        {
            "name": "deny_values",
            "example": "machine:'press9'",
            "description": "Membership deny-list, same form as allow_values; a deny match wins over an allow match. Default none.",
            "required": false
        },
        {
            "name": "sparkplug_device_timeout",
            "example": "0",
            "description": "Publish DDEATH for a device that has produced no rows for this long, and DBIRTH again when it returns. Duration or 0. Default 0, which never publishes DDEATH because an idle device cannot be told from a removed one.",
            "required": false
        },
        {
            "name": "sparkplug_primary_host_id",
            "example": "scada1",
            "description": "When set, the node subscribes to spBv1.0/STATE/<id> (QoS 1), waits for 'online' before its first NBIRTH, and on 'offline' publishes NDEATH and reconnects. Default empty (no STATE handling). Rebuilds the client.",
            "required": false
        },
        {
            "name": "live_window",
            "example": "5min",
            "description": "A row whose data timestamp is older than now minus this is published with is_historical=true instead of as the node's current value, which keeps backfill, late data and a WAL replay from rolling the live value back. Duration or 0 (test off). Default 5min.",
            "required": false
        },
        {
            "name": "sparkplug_replay_historical",
            "example": "true",
            "description": "What happens to rows marked historical by live_window. 'true' (default) republishes them with is_historical=true; 'false' discards them. Live rows are unaffected either way.",
            "required": false
        },
        {
            "name": "report_by_exception",
            "example": "false",
            "description": "When 'true', a metric is published only if its value differs from the last one published for that alias, the Sparkplug-recommended mode for periodically-polled sources. Default 'false' (every row is published as written).",
            "required": false
        },
        {
            "name": "deadband",
            "example": "0",
            "description": "With report_by_exception, a numeric change smaller than this counts as no change, to suppress analog noise. Number >= 0, default 0 (publish on any change).",
            "required": false
        },
        {
            "name": "max_payload_bytes",
            "example": "122880",
            "description": "Largest message built, counting topic and properties. A per-device batch is split to stay under it, and the birth size is checked against it before connecting. Integer >= 1024, default 122880. Clamped by an MQTT 5 broker's Maximum Packet Size.",
            "required": false
        },
        {
            "name": "max_queue_bytes",
            "example": "67108864",
            "description": "The only bound on the in-memory buffer, charged against the Python footprint of the buffered entries (not the wire size), so it bounds real process memory. Integer >= 1024, default 67108864. Per trigger, shared engine process.",
            "required": false
        },
        {
            "name": "drop_policy",
            "example": "drop_oldest",
            "description": "What happens at the buffer cap: 'drop_oldest' (default) or 'drop_newest'.",
            "required": false
        },
        {
            "name": "drain_order",
            "example": "fifo",
            "description": "'fifo' (default) drains strictly in order, so after a long outage live data waits behind the history. 'live_first' publishes recent rows first and drains historical ones with the leftover rate budget, so current values stay prompt.",
            "required": false
        },
        {
            "name": "max_message_age",
            "example": "0",
            "description": "On drain, a buffered entry older than this is discarded instead of published. Duration or 0 (never discard). Default 0.",
            "required": false
        },
        {
            "name": "max_messages_per_second",
            "example": "0",
            "description": "Token-bucket cap on how fast the sender hands messages over. Integer >= 1 or 0 (no limit). Default 0.",
            "required": false
        },
        {
            "name": "max_unconfirmed_bytes",
            "example": "16777216",
            "description": "Back-pressure threshold, as the total size handed over and not yet reported back by on_publish. Integer >= 65536, default 16777216.",
            "required": false
        },
        {
            "name": "config_file_path",
            "example": "mqtt_sparkplug_publisher_config.toml",
            "description": "TOML file under PLUGIN_DIR, read after the environment and trigger arguments and winning over both. The place for any value containing a comma, and for credentials that should not appear in the trigger catalogue.",
            "required": false
        },
        {
            "name": "stats_interval",
            "example": "1min",
            "description": "How often counters are written to mqtt_sparkplug_publisher_stats and summarised in the log. Duration or 0 (write no measurement, keep the log summary). Default 1min.",
            "required": false
        },
        {
            "name": "enable_full_logging",
            "example": "false",
            "description": "When false (default), an exception is logged as its type only; reason codes, errno and the setting an error names are logged either way. Credentials are never logged under either setting.",
            "required": false
        }
    ]
}
"""

import base64
import hashlib
import json
import socket
import ssl
import threading
import time
import uuid
from collections import deque
from urllib.parse import urlparse

import paho.mqtt.client as mqtt

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

from influxdata_plugin_utils.config import load_config, resolve_path
from influxdata_plugin_utils.introspection import TAG_DATA_TYPE, get_schema
from influxdata_plugin_utils.parsing import (
    parse_bool,
    parse_delimited_list,
    parse_int,
    parse_timedelta,
)
from influxdata_plugin_utils.sources import (
    KeySpec,
    parse_env,
    parse_toml,
    parse_trigger_args,
)
from influxdata_plugin_utils.validation import Validator

# --- constants --------------------------------------------------------------

STATE_CACHE_KEY = "mqtt_sparkplug_publisher_state"
STATS_MEASUREMENT = "mqtt_sparkplug_publisher_stats"

# Rebirth requests arriving within this window collapse into one birth
# sequence, so a faulty host cannot storm the node (4.2).
REBIRTH_COALESCE_SECONDS = 5.0

# Per-entry and per-metric Python footprint charged to the buffer, so
# max_queue_bytes bounds real memory rather than the wire size (D-1).
BUFFER_ENTRY_OVERHEAD = 256
BUFFER_METRIC_OVERHEAD = 96


# --- codec ------------------------------------------------------------------
#
# The Sparkplug Payload type is built at runtime from the compiled
# FileDescriptorSet embedded below, rather than from a generated
# sparkplug_b_pb2.py or a codec library such as pysparkplug. Two reasons:
#   - the whole plugin ships as a single file, with no second generated module
#     to keep in sync;
#   - every plugin on a node shares one virtual environment, so pinning a
#     protobuf major (as generated code effectively does) or pulling pysparkplug
#     (which pins paho-mqtt<2) would downgrade dependencies that mqtt_publisher
#     and mqtt_subscriber also rely on and break them. The embedded descriptor
#     is version-agnostic and loads under any modern protobuf.
# Regenerate the constant from the shipped sparkplug_b.proto with:
#
#   python -m grpc_tools.protoc -I. --descriptor_set_out=sparkplug_b.desc sparkplug_b.proto
#   base64 -w0 sparkplug_b.desc
#
# spBv1.0 has not changed since it was defined, so this is the only codec
# maintenance ever needed.

_SPARKPLUG_DESCRIPTOR_B64 = "CtsgChFzcGFya3BsdWdfYi5wcm90bxIZb3JnLmVjbGlwc2UudGFodS5wcm90b2J1ZiKHHAoHUGF5bG9hZBIcCgl0aW1lc3RhbXAYASABKARSCXRpbWVzdGFtcBJDCgdtZXRyaWNzGAIgAygLMikub3JnLmVjbGlwc2UudGFodS5wcm90b2J1Zi5QYXlsb2FkLk1ldHJpY1IHbWV0cmljcxIQCgNzZXEYAyABKARSA3NlcRISCgR1dWlkGAQgASgJUgR1dWlkEhIKBGJvZHkYBSABKAxSBGJvZHkaxAUKCFRlbXBsYXRlEhgKB3ZlcnNpb24YASABKAlSB3ZlcnNpb24SQwoHbWV0cmljcxgCIAMoCzIpLm9yZy5lY2xpcHNlLnRhaHUucHJvdG9idWYuUGF5bG9hZC5NZXRyaWNSB21ldHJpY3MSVQoKcGFyYW1ldGVycxgDIAMoCzI1Lm9yZy5lY2xpcHNlLnRhaHUucHJvdG9idWYuUGF5bG9hZC5UZW1wbGF0ZS5QYXJhbWV0ZXJSCnBhcmFtZXRlcnMSIQoMdGVtcGxhdGVfcmVmGAQgASgJUgt0ZW1wbGF0ZVJlZhIjCg1pc19kZWZpbml0aW9uGAUgASgIUgxpc0RlZmluaXRpb24arwMKCVBhcmFtZXRlchISCgRuYW1lGAEgASgJUgRuYW1lEhIKBHR5cGUYAiABKA1SBHR5cGUSHQoJaW50X3ZhbHVlGAMgASgNSABSCGludFZhbHVlEh8KCmxvbmdfdmFsdWUYBCABKARIAFIJbG9uZ1ZhbHVlEiEKC2Zsb2F0X3ZhbHVlGAUgASgCSABSCmZsb2F0VmFsdWUSIwoMZG91YmxlX3ZhbHVlGAYgASgBSABSC2RvdWJsZVZhbHVlEiUKDWJvb2xlYW5fdmFsdWUYByABKAhIAFIMYm9vbGVhblZhbHVlEiMKDHN0cmluZ192YWx1ZRgIIAEoCUgAUgtzdHJpbmdWYWx1ZRJ4Cg9leHRlbnNpb25fdmFsdWUYCSABKAsyTS5vcmcuZWNsaXBzZS50YWh1LnByb3RvYnVmLlBheWxvYWQuVGVtcGxhdGUuUGFyYW1ldGVyLlBhcmFtZXRlclZhbHVlRXh0ZW5zaW9uSABSDmV4dGVuc2lvblZhbHVlGiMKF1BhcmFtZXRlclZhbHVlRXh0ZW5zaW9uKggIARCAgICAAkIHCgV2YWx1ZSoICAYQgICAgAIangUKB0RhdGFTZXQSJAoObnVtX29mX2NvbHVtbnMYASABKARSDG51bU9mQ29sdW1ucxIYCgdjb2x1bW5zGAIgAygJUgdjb2x1bW5zEhQKBXR5cGVzGAMgAygNUgV0eXBlcxJCCgRyb3dzGAQgAygLMi4ub3JnLmVjbGlwc2UudGFodS5wcm90b2J1Zi5QYXlsb2FkLkRhdGFTZXQuUm93UgRyb3dzGogDCgxEYXRhU2V0VmFsdWUSHQoJaW50X3ZhbHVlGAEgASgNSABSCGludFZhbHVlEh8KCmxvbmdfdmFsdWUYAiABKARIAFIJbG9uZ1ZhbHVlEiEKC2Zsb2F0X3ZhbHVlGAMgASgCSABSCmZsb2F0VmFsdWUSIwoMZG91YmxlX3ZhbHVlGAQgASgBSABSC2RvdWJsZVZhbHVlEiUKDWJvb2xlYW5fdmFsdWUYBSABKAhIAFIMYm9vbGVhblZhbHVlEiMKDHN0cmluZ192YWx1ZRgGIAEoCUgAUgtzdHJpbmdWYWx1ZRJ4Cg9leHRlbnNpb25fdmFsdWUYByABKAsyTS5vcmcuZWNsaXBzZS50YWh1LnByb3RvYnVmLlBheWxvYWQuRGF0YVNldC5EYXRhU2V0VmFsdWUuRGF0YVNldFZhbHVlRXh0ZW5zaW9uSABSDmV4dGVuc2lvblZhbHVlGiEKFURhdGFTZXRWYWx1ZUV4dGVuc2lvbioICAEQgICAgAJCBwoFdmFsdWUaZAoDUm93ElMKCGVsZW1lbnRzGAEgAygLMjcub3JnLmVjbGlwc2UudGFodS5wcm90b2J1Zi5QYXlsb2FkLkRhdGFTZXQuRGF0YVNldFZhbHVlUghlbGVtZW50cyoICAIQgICAgAIqCAgFEICAgIACGvUECg1Qcm9wZXJ0eVZhbHVlEhIKBHR5cGUYASABKA1SBHR5cGUSFwoHaXNfbnVsbBgCIAEoCFIGaXNOdWxsEh0KCWludF92YWx1ZRgDIAEoDUgAUghpbnRWYWx1ZRIfCgpsb25nX3ZhbHVlGAQgASgESABSCWxvbmdWYWx1ZRIhCgtmbG9hdF92YWx1ZRgFIAEoAkgAUgpmbG9hdFZhbHVlEiMKDGRvdWJsZV92YWx1ZRgGIAEoAUgAUgtkb3VibGVWYWx1ZRIlCg1ib29sZWFuX3ZhbHVlGAcgASgISABSDGJvb2xlYW5WYWx1ZRIjCgxzdHJpbmdfdmFsdWUYCCABKAlIAFILc3RyaW5nVmFsdWUSXQoRcHJvcGVydHlzZXRfdmFsdWUYCSABKAsyLi5vcmcuZWNsaXBzZS50YWh1LnByb3RvYnVmLlBheWxvYWQuUHJvcGVydHlTZXRIAFIQcHJvcGVydHlzZXRWYWx1ZRJjChJwcm9wZXJ0eXNldHNfdmFsdWUYCiABKAsyMi5vcmcuZWNsaXBzZS50YWh1LnByb3RvYnVmLlBheWxvYWQuUHJvcGVydHlTZXRMaXN0SABSEXByb3BlcnR5c2V0c1ZhbHVlEnIKD2V4dGVuc2lvbl92YWx1ZRgLIAEoCzJHLm9yZy5lY2xpcHNlLnRhaHUucHJvdG9idWYuUGF5bG9hZC5Qcm9wZXJ0eVZhbHVlLlByb3BlcnR5VmFsdWVFeHRlbnNpb25IAFIOZXh0ZW5zaW9uVmFsdWUaIgoWUHJvcGVydHlWYWx1ZUV4dGVuc2lvbioICAEQgICAgAJCBwoFdmFsdWUadQoLUHJvcGVydHlTZXQSEgoEa2V5cxgBIAMoCVIEa2V5cxJICgZ2YWx1ZXMYAiADKAsyMC5vcmcuZWNsaXBzZS50YWh1LnByb3RvYnVmLlBheWxvYWQuUHJvcGVydHlWYWx1ZVIGdmFsdWVzKggIAxCAgICAAhptCg9Qcm9wZXJ0eVNldExpc3QSUAoLcHJvcGVydHlzZXQYASADKAsyLi5vcmcuZWNsaXBzZS50YWh1LnByb3RvYnVmLlBheWxvYWQuUHJvcGVydHlTZXRSC3Byb3BlcnR5c2V0KggIAhCAgICAAhrvAQoITWV0YURhdGESIgoNaXNfbXVsdGlfcGFydBgBIAEoCFILaXNNdWx0aVBhcnQSIQoMY29udGVudF90eXBlGAIgASgJUgtjb250ZW50VHlwZRISCgRzaXplGAMgASgEUgRzaXplEhAKA3NlcRgEIAEoBFIDc2VxEhsKCWZpbGVfbmFtZRgFIAEoCVIIZmlsZU5hbWUSGwoJZmlsZV90eXBlGAYgASgJUghmaWxlVHlwZRIQCgNtZDUYByABKAlSA21kNRIgCgtkZXNjcmlwdGlvbhgIIAEoCVILZGVzY3JpcHRpb24qCAgJEICAgIACGpwHCgZNZXRyaWMSEgoEbmFtZRgBIAEoCVIEbmFtZRIUCgVhbGlhcxgCIAEoBFIFYWxpYXMSHAoJdGltZXN0YW1wGAMgASgEUgl0aW1lc3RhbXASGgoIZGF0YXR5cGUYBCABKA1SCGRhdGF0eXBlEiMKDWlzX2hpc3RvcmljYWwYBSABKAhSDGlzSGlzdG9yaWNhbBIhCgxpc190cmFuc2llbnQYBiABKAhSC2lzVHJhbnNpZW50EhcKB2lzX251bGwYByABKAhSBmlzTnVsbBJHCghtZXRhZGF0YRgIIAEoCzIrLm9yZy5lY2xpcHNlLnRhaHUucHJvdG9idWYuUGF5bG9hZC5NZXRhRGF0YVIIbWV0YWRhdGESTgoKcHJvcGVydGllcxgJIAEoCzIuLm9yZy5lY2xpcHNlLnRhaHUucHJvdG9idWYuUGF5bG9hZC5Qcm9wZXJ0eVNldFIKcHJvcGVydGllcxIdCglpbnRfdmFsdWUYCiABKA1IAFIIaW50VmFsdWUSHwoKbG9uZ192YWx1ZRgLIAEoBEgAUglsb25nVmFsdWUSIQoLZmxvYXRfdmFsdWUYDCABKAJIAFIKZmxvYXRWYWx1ZRIjCgxkb3VibGVfdmFsdWUYDSABKAFIAFILZG91YmxlVmFsdWUSJQoNYm9vbGVhbl92YWx1ZRgOIAEoCEgAUgxib29sZWFuVmFsdWUSIwoMc3RyaW5nX3ZhbHVlGA8gASgJSABSC3N0cmluZ1ZhbHVlEiEKC2J5dGVzX3ZhbHVlGBAgASgMSABSCmJ5dGVzVmFsdWUSUQoNZGF0YXNldF92YWx1ZRgRIAEoCzIqLm9yZy5lY2xpcHNlLnRhaHUucHJvdG9idWYuUGF5bG9hZC5EYXRhU2V0SABSDGRhdGFzZXRWYWx1ZRJUCg50ZW1wbGF0ZV92YWx1ZRgSIAEoCzIrLm9yZy5lY2xpcHNlLnRhaHUucHJvdG9idWYuUGF5bG9hZC5UZW1wbGF0ZUgAUg10ZW1wbGF0ZVZhbHVlEmkKD2V4dGVuc2lvbl92YWx1ZRgTIAEoCzI+Lm9yZy5lY2xpcHNlLnRhaHUucHJvdG9idWYuUGF5bG9hZC5NZXRyaWMuTWV0cmljVmFsdWVFeHRlbnNpb25IAFIOZXh0ZW5zaW9uVmFsdWUaIAoUTWV0cmljVmFsdWVFeHRlbnNpb24qCAgBEICAgIACQgcKBXZhbHVlKggIBhCAgICAAiryAwoIRGF0YVR5cGUSCwoHVW5rbm93bhAAEggKBEludDgQARIJCgVJbnQxNhACEgkKBUludDMyEAMSCQoFSW50NjQQBBIJCgVVSW50OBAFEgoKBlVJbnQxNhAGEgoKBlVJbnQzMhAHEgoKBlVJbnQ2NBAIEgkKBUZsb2F0EAkSCgoGRG91YmxlEAoSCwoHQm9vbGVhbhALEgoKBlN0cmluZxAMEgwKCERhdGVUaW1lEA0SCAoEVGV4dBAOEggKBFVVSUQQDxILCgdEYXRhU2V0EBASCQoFQnl0ZXMQERIICgRGaWxlEBISDAoIVGVtcGxhdGUQExIPCgtQcm9wZXJ0eVNldBAUEhMKD1Byb3BlcnR5U2V0TGlzdBAVEg0KCUludDhBcnJheRAWEg4KCkludDE2QXJyYXkQFxIOCgpJbnQzMkFycmF5EBgSDgoKSW50NjRBcnJheRAZEg4KClVJbnQ4QXJyYXkQGhIPCgtVSW50MTZBcnJheRAbEg8KC1VJbnQzMkFycmF5EBwSDwoLVUludDY0QXJyYXkQHRIOCgpGbG9hdEFycmF5EB4SDwoLRG91YmxlQXJyYXkQHxIQCgxCb29sZWFuQXJyYXkQIBIPCgtTdHJpbmdBcnJheRAhEhEKDURhdGVUaW1lQXJyYXkQIkIsChlvcmcuZWNsaXBzZS50YWh1LnByb3RvYnVmQg9TcGFya3BsdWdCUHJvdG8="  # noqa: E501


def build_payload_class():
    """Build the Sparkplug ``Payload`` message class from the embedded descriptor.

    A private descriptor pool is used so two client builds in one process —
    across invocations, or across triggers — never clash in the global pool.
    """
    file_set = descriptor_pb2.FileDescriptorSet()
    file_set.ParseFromString(base64.b64decode(_SPARKPLUG_DESCRIPTOR_B64))
    pool = descriptor_pool.DescriptorPool()
    for file_proto in file_set.file:
        pool.Add(file_proto)
    message_descriptor = pool.FindMessageTypeByName("org.eclipse.tahu.protobuf.Payload")
    return message_factory.GetMessageClass(message_descriptor)


# --- logging ----------------------------------------------------------------


def describe_error(exc: BaseException, full_logging: bool) -> str:
    """Describe an exception without leaking what the settings contain.

    The diagnosis is never withheld — the exception type, ``errno`` and
    ``strerror`` carry no secret. Only the free-text message is withheld
    unless ``enable_full_logging`` is set, because it can echo a broker URL
    or a credential.
    """
    parts = [type(exc).__name__]
    errno = getattr(exc, "errno", None)
    if errno is not None:
        parts.append(f"errno={errno}")
    strerror = getattr(exc, "strerror", None)
    if strerror:
        parts.append(str(strerror))
    if full_logging:
        message = str(exc)
        if message:
            parts.append(message)
    return " ".join(parts)


# --- configuration ----------------------------------------------------------

ENV_PREFIX = "INFLUXDB3_MQTT_SPARKPLUG_PUBLISHER_"
DEFAULT_PORTS = {"mqtt": 1883, "mqtts": 8883, "ws": 80, "wss": 443}
COMPARISON_OPERATORS = (">=", "<=", "!=", ">", "<", "=")

# Reserved in a Sparkplug topic token (Group/Edge Node/Device ID).
RESERVED_TOPIC_CHARS = ("/", "+", "#", "\x00")

# Sparkplug DataType enum values for the field types a metric may declare.
SPARKPLUG_DATATYPE_BY_NAME = {
    "int8": 1, "int16": 2, "int32": 3, "int64": 4,
    "uint8": 5, "uint16": 6, "uint32": 7, "uint64": 8,
    "float": 9, "double": 10, "boolean": 11, "string": 12, "datetime": 13,
}

# Source time units accepted on a 'datetime' metric, each converted to the
# milliseconds Sparkplug DateTime requires. The default is milliseconds.
DATETIME_UNITS = ("s", "ms", "us", "ns")


def typed_literal(raw):
    """Read a filter literal as an int, a float or a string."""
    text = str(raw).strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in ("'", '"'):
        return text[1:-1]
    for cast in (int, float):
        try:
            return cast(text)
        except ValueError:
            continue
    return text


def split_outside_quotes(raw: str) -> list:
    """Split on whitespace that is not inside quotes.

    One separator serves both levels: a space separates the pairs, and a space
    inside quotes separates the values of a list.
    """
    parts: list = []
    current: list = []
    quote = None
    for char in raw:
        if quote is not None:
            current.append(char)
            if char == quote:
                quote = None
        elif char in "'\"":
            current.append(char)
            quote = char
        elif char.isspace():
            if current:
                parts.append("".join(current))
                current = []
        else:
            current.append(char)
    if quote is not None:
        raise ValueError(f"unbalanced {quote} quote in a quoted value")  # no raw echo (finding 9)
    if current:
        parts.append("".join(current))
    return parts


def parse_pairs(raw) -> list:
    """Split ``col:'expr' col2:'expr'`` into ``(column, expression)`` pairs."""
    pairs: list = []
    for part in split_outside_quotes(str(raw)):
        if ":" not in part:
            raise ValueError(
                "a pair has no ':' between the column and the value; pairs are "
                "separated by spaces, as in \"temp:'>=185' state:'!=error'\""
            )  # no raw echo, the value may be a credential (finding 9)
        column, expression = part.split(":", 1)
        column = column.strip()
        expression = expression.strip()
        if len(expression) >= 2 and expression[0] == expression[-1] and expression[0] in ("'", '"'):
            expression = expression[1:-1]
        if not column:
            raise ValueError("a pair names no column before its ':'")
        pairs.append((column, expression))
    return pairs


def require_operator(column: str, operator: str) -> str:
    """Refuse a comparison operator the filter grammar does not define."""
    if operator not in COMPARISON_OPERATORS:
        raise ValueError(
            f"filter on {column!r} uses {operator!r}; expected one of "
            f"{', '.join(COMPARISON_OPERATORS)}"
        )
    return operator


def parse_filters(raw) -> list:
    """Parse filters from the compact string or a structured list of triples."""
    if not raw:
        return []
    if isinstance(raw, (list, tuple)):
        filters: list = []
        for entry in raw:
            if isinstance(entry, dict):
                missing = {"column", "operator", "value"} - set(entry)
                if missing:
                    raise ValueError(f"filter {entry!r} is missing {', '.join(sorted(missing))}")
                column, operator, value = entry["column"], entry["operator"], entry["value"]
            elif isinstance(entry, (list, tuple)) and len(entry) == 3:
                column, operator, value = entry
            else:
                raise ValueError(f"filter {entry!r} is not a [column, operator, value] triple")
            column = str(column).strip()
            operator = require_operator(column, str(operator).strip())
            filters.append(
                (column, operator, typed_literal(value) if isinstance(value, str) else value)
            )
        return filters
    filters = []
    for column, expression in parse_pairs(raw):
        operator = next(
            (op for op in COMPARISON_OPERATORS if expression.startswith(op)), None
        )
        if operator is None:
            raise ValueError(
                f"filter on {column!r} starts with no operator; expected one of "
                f"{', '.join(COMPARISON_OPERATORS)}"
            )
        filters.append((column, operator, typed_literal(expression[len(operator):])))
    return filters


def parse_header_map(raw) -> dict:
    """Parse WebSocket headers from ``name:'value'`` pairs or a table."""
    if not raw:
        return {}
    if isinstance(raw, dict):
        return {str(name): str(value) for name, value in raw.items()}
    return dict(parse_pairs(raw))


def parse_value_lists(raw) -> dict:
    """Parse membership tests from the compact string or a table."""
    if not raw:
        return {}
    if isinstance(raw, dict):
        return {
            str(column): [str(value) for value in parse_delimited_list(values)]
            for column, values in raw.items()
        }
    return {column: parse_delimited_list(expr) for column, expr in parse_pairs(raw)}


def trimmed(value) -> str:
    return str(value).strip()


def keyword(value) -> str:
    return str(value).strip().lower()


def duration_seconds(value) -> float:
    """Read a duration as seconds, accepting ``30s`` and a bare number alike."""
    if value in (None, "", 0, "0"):
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return parse_timedelta(value).total_seconds()


def nonneg_number(value) -> float:
    """Read a non-negative number, for the deadband."""
    number = float(value)
    if number < 0:
        raise ValueError("must be >= 0")
    return number


def version_text(value) -> str:
    text = str(value).strip()
    return {"3.1.1": "3.1.1", "311": "3.1.1", "5.0": "5", "5": "5"}.get(text, text)


SETTING_VALIDATORS: list = [
    # Connection
    Validator("broker_url", required=True, cast=trimmed),
    Validator("sparkplug_group_id", required=True, cast=trimmed),
    Validator("sparkplug_edge_node_id", required=True, cast=trimmed),
    Validator("source_measurements", required=True, cast=parse_delimited_list),
    Validator("payload_fields", required=True, cast=parse_delimited_list),
    Validator("sparkplug_device_from", default="", cast=trimmed),
    Validator("client_id", default="", cast=trimmed),
    Validator("mqtt_version", default="3.1.1", cast=version_text, is_in=["3.1.1", "5"]),
    Validator("username", default="", cast=trimmed),
    Validator("password", default="", cast=str),
    Validator("keep_alive", default=60, cast=parse_int, gte=1),
    Validator("tls_ca_cert", default="", cast=trimmed),
    Validator("tls_client_cert", default="", cast=trimmed),
    Validator("tls_client_key", default="", cast=trimmed),
    Validator("tls_insecure_skip_verify", default=False, cast=parse_bool),
    Validator("tls_alpn", default="", cast=trimmed),
    Validator("ws_path", default="", cast=trimmed),
    Validator("ws_headers", default={}, cast=parse_header_map),
    Validator("reconnect_min_backoff", default=1.0, cast=duration_seconds, gte=1.0),
    Validator("reconnect_max_backoff", default=120.0, cast=duration_seconds, gte=1.0),
    Validator("takeover_attempts", default=3, cast=parse_int, gte=2),
    Validator("idle_disconnect", default=3600.0, cast=duration_seconds, gte=60.0),
    # Source
    Validator("filters", default=[], cast=parse_filters),
    Validator("allow_values", default={}, cast=parse_value_lists),
    Validator("deny_values", default={}, cast=parse_value_lists),
    # Sparkplug
    Validator("sparkplug_device_timeout", default=0.0, cast=duration_seconds, gte=0.0),
    Validator("sparkplug_primary_host_id", default="", cast=trimmed),
    Validator("live_window", default=300.0, cast=duration_seconds, gte=0.0),
    Validator("sparkplug_replay_historical", default=True, cast=parse_bool),
    Validator("report_by_exception", default=False, cast=parse_bool),
    Validator("deadband", default=0.0, cast=nonneg_number),
    # Delivery
    Validator("max_payload_bytes", default=122880, cast=parse_int, gte=1024),
    Validator("max_queue_bytes", default=67108864, cast=parse_int, gte=1024),
    Validator("drop_policy", default="drop_oldest", cast=keyword, is_in=["drop_oldest", "drop_newest"]),
    Validator("drain_order", default="fifo", cast=keyword, is_in=["fifo", "live_first"]),
    Validator("max_message_age", default=0.0, cast=duration_seconds, gte=0.0),
    Validator("max_messages_per_second", default=0, cast=parse_int, gte=0),
    Validator("max_unconfirmed_bytes", default=16777216, cast=parse_int, gte=65536),
    # Common
    Validator("stats_interval", default=60.0, cast=duration_seconds, gte=0.0),
    Validator("enable_full_logging", default=False, cast=parse_bool),
]

SETTING_NAMES = tuple(
    dict.fromkeys(name for validator in SETTING_VALIDATORS for name in validator.names)
)


def env_spec(*names: str) -> KeySpec:
    """Read settings from ``INFLUXDB3_MQTT_SPARKPLUG_PUBLISHER_<SETTING>``."""
    rename = {f"{ENV_PREFIX}{name.upper()}": name for name in names}
    return KeySpec(allowlist=tuple(rename), rename=rename)


SETTING_ENV = env_spec(*SETTING_NAMES)

# A misspelled setting that silently does nothing costs an afternoon, so the
# layers the operator controls name what they accept and refuse the rest.
# config_file_path names a layer, not a value, so only an argument may set it.
ARG_KEYS = KeySpec(allowlist=SETTING_NAMES + ("config_file_path",), unknown="reject")
TOML_KEYS = KeySpec(allowlist=SETTING_NAMES, unknown="reject")


def parse_broker_url(raw: str) -> dict:
    """Split ``broker_url`` into transport, TLS, host, port and path."""
    text = str(raw).strip()
    if "://" not in text:
        text = "mqtt://" + text
    parsed = urlparse(text)
    scheme = (parsed.scheme or "mqtt").lower()
    if scheme == "unix":
        # paho ignores the port on the unix transport, but connect_async still
        # refuses a non-positive one, so pass a stub (finding 6).
        return {"transport": "unix", "tls": False, "host": parsed.path, "port": 1883, "path": ""}
    if scheme not in DEFAULT_PORTS:
        raise ValueError(f"scheme {scheme!r} is not one of mqtt, mqtts, ws, wss, unix")
    if not parsed.hostname:
        raise ValueError("names no host")  # no raw echo; broker_url may carry userinfo (finding 9)
    return {
        "transport": "websockets" if scheme in ("ws", "wss") else "tcp",
        "tls": scheme in ("mqtts", "wss"),
        "host": parsed.hostname,
        "port": parsed.port or DEFAULT_PORTS[scheme],
        "path": parsed.path or "/mqtt",
    }


def derive_client_id(group_id: str, edge_node_id: str) -> str:
    """A stable default client id, so the broker evicts an orphan by takeover."""
    digest = hashlib.sha1(f"{group_id}|{edge_node_id}".encode()).hexdigest()[:8]
    return f"influxdb3-spb-{digest}"


def reserved_char(value: str):
    """Return the first reserved topic character in value, or None."""
    for char in RESERVED_TOPIC_CHARS:
        if char in value:
            return char
    return None


def check_settings(cfg: dict) -> None:
    """Config-only refusals; schema-dependent checks happen at client build."""
    try:
        parse_broker_url(cfg["broker_url"])
    except ValueError as exc:
        raise ValueError(f"broker_url: {exc}") from None

    # Group and Edge Node IDs become topic tokens, so they are validated as one.
    for setting in ("sparkplug_group_id", "sparkplug_edge_node_id"):
        value = cfg[setting]
        bad = "$" if value.startswith("$") else reserved_char(value)
        if bad is not None:
            raise ValueError(
                f"{setting}: {value!r} contains a reserved topic character {bad!r}"
            )

    # Unreadable TLS material never fixes itself by retrying; the path is named,
    # its contents never are. An encrypted key would block the handshake.
    for setting in ("tls_ca_cert", "tls_client_cert", "tls_client_key"):
        path = cfg[setting]
        if not path:
            continue
        try:
            material = resolve_path(path).read_bytes()
        except OSError as exc:
            raise ValueError(
                f"{setting}: cannot read {path!r} ({exc.strerror or type(exc).__name__})"
            ) from None
        if setting == "tls_client_key" and b"ENCRYPTED" in material:
            raise ValueError(
                f"tls_client_key: {path!r} is passphrase-protected, which is not "
                f"supported; provide an unencrypted key"
            )
    if bool(cfg["tls_client_cert"]) != bool(cfg["tls_client_key"]):
        raise ValueError("mutual TLS needs both 'tls_client_cert' and 'tls_client_key'")

    # Device mapping: empty, 'table', or 'tag:<name>'.
    device_from = cfg["sparkplug_device_from"]
    if device_from not in ("", "table") and not device_from.startswith("tag:"):
        raise ValueError(
            "sparkplug_device_from must be empty, 'table', or 'tag:<name>'"
        )
    if device_from.startswith("tag:"):
        if not device_from[len("tag:"):].strip():
            raise ValueError("sparkplug_device_from='tag:' names no tag")
        if len(cfg["source_measurements"]) != 1:
            raise ValueError(
                "sparkplug_device_from='tag:<name>' requires exactly one source table"
            )

    # payload_fields entries are 'name', 'name:type', or 'name:datetime:unit'.
    for entry in cfg["payload_fields"]:
        if len(entry.split(":")) > 3:
            raise ValueError(f"payload_fields entry {entry!r} has too many ':' segments")
        name, override, unit = split_field_type(entry)
        if not name:
            raise ValueError(f"payload_fields entry {entry!r} names no column")
        if override and override not in SPARKPLUG_DATATYPE_BY_NAME:
            raise ValueError(
                f"payload_fields entry {entry!r} has unknown type {override!r}; "
                f"expected one of {', '.join(sorted(SPARKPLUG_DATATYPE_BY_NAME))}"
            )
        if unit is not None:
            if override != "datetime":
                raise ValueError(
                    f"payload_fields entry {entry!r}: a time unit is only valid on a 'datetime' field"
                )
            if unit not in DATETIME_UNITS:
                raise ValueError(
                    f"payload_fields entry {entry!r} has unknown datetime unit {unit!r}; "
                    f"expected one of {', '.join(DATETIME_UNITS)}"
                )

    if cfg["reconnect_max_backoff"] < cfg["reconnect_min_backoff"]:
        raise ValueError("reconnect_max_backoff must be >= reconnect_min_backoff")


def load_configuration(influxdb3_local, args: dict, task_id: str) -> dict:
    """Layer env, trigger arguments and the TOML file, then validate."""
    config_file_path = args.get("config_file_path") or parse_env(
        env_spec("config_file_path")
    ).get("config_file_path")
    cfg = load_config(
        parse_env(SETTING_ENV),
        parse_trigger_args(args, ARG_KEYS),
        parse_toml(config_file_path, TOML_KEYS),
        validators=SETTING_VALIDATORS,
    )
    check_settings(cfg)
    if not cfg["client_id"]:
        cfg["client_id"] = derive_client_id(
            cfg["sparkplug_group_id"], cfg["sparkplug_edge_node_id"]
        )
    return cfg


# --- Sparkplug encoding -----------------------------------------------------
#
# The ``session`` dict (held in the cached state) carries the encoding state:
#   bdseq, seq            the current birth/death and message sequence numbers
#   aliases               {(device, name): alias} — a running counter per node
#   next_alias            the next alias to hand out
#   last_value            {alias: (value, ts_ms, is_null)} for birth and RBE
#   known_devices         {device: last_seen_monotonic} for DBIRTH / timeout
#   model                 the metric model from build_metric_model()

# InfluxDB column kind (from get_schema) to a Sparkplug DataType name.
INFLUX_KIND_TO_SPARKPLUG = {
    "Int64": "int64",
    "Int32": "int32",
    "UInt64": "uint64",
    "Float64": "double",
    "Float32": "float",
    "Boolean": "boolean",
    "Utf8": "string",
}

# DataType enum values whose value rides in int_value (uint32) vs long_value
# (uint64); a signed value is masked to the field width (3.1).
_INT32_VALUE_TYPES = frozenset({1, 2, 3, 5, 6, 7})  # Int8/16/32, UInt8/16/32
_INT64_VALUE_TYPES = frozenset({4, 8, 13})  # Int64, UInt64, DateTime
_BOOLEAN_TYPE = 11
_STRING_TYPE = 12
_FLOAT_TYPE = 9
_DOUBLE_TYPE = 10


def ms_from_ns(timestamp_ns: int) -> int:
    """Sparkplug milliseconds from InfluxDB nanoseconds (sub-ms precision lost)."""
    return int(timestamp_ns) // 1_000_000


def datetime_to_ms(value, unit: str):
    """Convert a datetime field value from its source unit to milliseconds."""
    number = int(value)
    if unit == "s":
        return number * 1000
    if unit == "us":
        return number // 1000
    if unit == "ns":
        return number // 1_000_000
    return number  # ms


def now_ms() -> int:
    """Wall-clock milliseconds, for a payload-level timestamp."""
    return int(time.time() * 1000)


def alias_for(session: dict, device: str, name: str) -> int:
    """Assign or fetch a node-wide-unique alias for a (device, metric) pair (S-3)."""
    key = (device, name)
    with session["lock"]:
        alias = session["aliases"].get(key)
        if alias is None:
            alias = session["next_alias"]
            session["aliases"][key] = alias
            session["next_alias"] = alias + 1
    return alias


def _join_topic(*tokens: str) -> str:
    return "/".join(tokens)


def nbirth_topic(cfg: dict) -> str:
    return _join_topic("spBv1.0", cfg["sparkplug_group_id"], "NBIRTH", cfg["sparkplug_edge_node_id"])


def ndeath_topic(cfg: dict) -> str:
    return _join_topic("spBv1.0", cfg["sparkplug_group_id"], "NDEATH", cfg["sparkplug_edge_node_id"])


def ndata_topic(cfg: dict) -> str:
    return _join_topic("spBv1.0", cfg["sparkplug_group_id"], "NDATA", cfg["sparkplug_edge_node_id"])


def ncmd_topic(cfg: dict) -> str:
    return _join_topic("spBv1.0", cfg["sparkplug_group_id"], "NCMD", cfg["sparkplug_edge_node_id"])


def dbirth_topic(cfg: dict, device: str) -> str:
    return _join_topic(
        "spBv1.0", cfg["sparkplug_group_id"], "DBIRTH", cfg["sparkplug_edge_node_id"], device
    )


def ddata_topic(cfg: dict, device: str) -> str:
    return _join_topic(
        "spBv1.0", cfg["sparkplug_group_id"], "DDATA", cfg["sparkplug_edge_node_id"], device
    )


def ddeath_topic(cfg: dict, device: str) -> str:
    return _join_topic(
        "spBv1.0", cfg["sparkplug_group_id"], "DDEATH", cfg["sparkplug_edge_node_id"], device
    )


def state_topic(cfg: dict) -> str:
    return _join_topic("spBv1.0", "STATE", cfg["sparkplug_primary_host_id"])


def set_metric_value(metric, datatype: int, value) -> None:
    """Set a metric's value for its datatype, or mark it null.

    Signed integers are written as two's-complement in the unsigned field at the
    field width (3.1); a narrowing override truncates a float toward zero (L-3).
    A value that cannot be coerced raises, so the sender's guard drops the row
    (A-6).
    """
    if value is None:
        metric.is_null = True
        return
    if datatype in _INT32_VALUE_TYPES:
        metric.int_value = int(value) & 0xFFFFFFFF
    elif datatype in _INT64_VALUE_TYPES:
        metric.long_value = int(value) & 0xFFFFFFFFFFFFFFFF
    elif datatype == _FLOAT_TYPE:
        metric.float_value = float(value)
    elif datatype == _DOUBLE_TYPE:
        metric.double_value = float(value)
    elif datatype == _BOOLEAN_TYPE:
        metric.boolean_value = bool(value)
    elif datatype == _STRING_TYPE:
        metric.string_value = str(value)
    else:
        raise ValueError(f"unsupported datatype {datatype}")


def split_field_type(entry: str):
    """Split a payload_fields entry into (field, type_override, datetime_unit).

    Grammar: ``name``, ``name:type``, or for a datetime ``name:datetime:unit``
    where unit is the source column's time unit (s, ms, us, ns). The override and
    the unit are ``None`` when absent.
    """
    parts = entry.split(":")
    field = parts[0].strip()
    override = parts[1].strip().lower() if len(parts) > 1 and parts[1].strip() else None
    unit = parts[2].strip().lower() if len(parts) > 2 and parts[2].strip() else None
    return field, override, unit


def resolve_datatype(field: str, type_override, table_types: dict):
    """Datatype enum for a field from its override, else the schema; None if unknown."""
    if type_override:
        return SPARKPLUG_DATATYPE_BY_NAME[type_override]
    kind = table_types.get(field)
    name = INFLUX_KIND_TO_SPARKPLUG.get(kind)
    return SPARKPLUG_DATATYPE_BY_NAME.get(name) if name else None


def build_metric_model(influxdb3_local, cfg: dict, refresh: bool = False) -> dict:
    """Build the metric model from the source-table schemas.

    Reads the tag/field split per table, refuses a tag that would merge several
    series onto one metric (3.2), and lists the declared metrics with their
    datatypes. A declared field with no known type yet (empty table, no override)
    is left out and rejoins the birth on the schema re-read (D-3). ``refresh``
    forces that re-read when a declared field first appears.
    """
    device_from = cfg["sparkplug_device_from"]
    if device_from.startswith("tag:"):
        mode, device_tag = "device_tag", device_from[len("tag:"):].strip()
    elif device_from == "table":
        mode, device_tag = "device_table", None
    else:
        mode, device_tag = "node", None

    pinned = {tag for tag, values in cfg["allow_values"].items() if len(values) == 1}
    node_metrics: list = []
    field_specs_by_table: dict = {}
    tags_by_table: dict = {}
    field_kinds: dict = {}

    declared_names = {split_field_type(entry)[0] for entry in cfg["payload_fields"]}
    for table in cfg["source_measurements"]:
        try:
            columns = get_schema(influxdb3_local, table, refresh=refresh)
        except ValueError:
            # The table does not exist yet; its metrics join once it is written, via
            # the D-3 schema re-read. Don't block the other tables (finding 12).
            tags_by_table[table] = []
            field_specs_by_table[table] = []
            influxdb3_local.info(f"table {table!r} not found yet; its metrics will join when it appears")
            continue
        tags = [name for name, kind in columns.items() if kind == TAG_DATA_TYPE]
        types = {
            name: kind
            for name, kind in columns.items()
            if kind != TAG_DATA_TYPE and name != "time"
        }
        tags_by_table[table] = tags
        field_kinds.update(types)

        # 3.2: a tag that is neither the device dimension nor pinned to one value
        # would collapse several series onto one metric name.
        for tag in tags:
            if tag == device_tag or tag in pinned:
                continue
            raise ValueError(
                f"tag {tag!r} in table {table!r} is neither the device tag nor pinned "
                f"to a single value in allow_values, so its series would merge onto one "
                f"metric; pin it in allow_values or make it the device"
            )

        specs: list = []
        for entry in cfg["payload_fields"]:
            field, override, unit = split_field_type(entry)
            datatype = resolve_datatype(field, override, types)
            if datatype is None:
                continue  # unknown type yet; rejoins on the schema re-read (D-3)
            datetime_unit = unit if override == "datetime" else None
            if mode == "node":
                node_metrics.append(
                    {"name": f"{table}/{field}", "field": field, "table": table,
                     "datatype": datatype, "datetime_unit": datetime_unit}
                )
            else:
                specs.append(
                    {"name": field, "field": field, "table": table,
                     "datatype": datatype, "datetime_unit": datetime_unit}
                )
        field_specs_by_table[table] = specs

        # A written field not in payload_fields is not published; note it once (§6).
        undeclared = [name for name in types if name not in declared_names]
        if undeclared:
            influxdb3_local.info(
                f"table {table!r}: columns {undeclared} are not in payload_fields and are not published"
            )

    # §6: an ordering filter on a column whose kind is known must compare like
    # with like; a numeric/text mismatch is a configuration error caught here.
    numeric_kinds = {"Int64", "Int32", "UInt64", "Float64", "Float32"}
    for column, operator, value in cfg["filters"]:
        kind = field_kinds.get(column)
        if kind is None or operator in ("=", "!="):
            continue
        column_numeric = kind in numeric_kinds
        value_numeric = isinstance(value, (int, float)) and not isinstance(value, bool)
        if column_numeric != value_numeric:
            raise ValueError(
                f"filter on {column!r} ({operator}) compares a "
                f"{'numeric' if column_numeric else 'text'} column with a "
                f"{'numeric' if value_numeric else 'text'} value"
            )

    return {
        "mode": mode,
        "device_tag": device_tag,
        "node_metrics": node_metrics,
        "field_specs_by_table": field_specs_by_table,
        "tags_by_table": tags_by_table,
    }


# --- Sparkplug payloads -----------------------------------------------------

# Control-metric datatypes and a conservative per-message overhead (MQTT fixed
# header plus MQTT 5 properties) added to a birth's size for the A-4 check.
_DT_INT64 = 4
_MESSAGE_OVERHEAD_BYTES = 64


def _birth_metric(payload, session: dict, name: str, alias: int, datatype: int, ts_ms: int) -> None:
    """Add a metric to a birth with its last published value, or null (1.3)."""
    metric = payload.metrics.add()
    metric.name = name
    metric.alias = alias
    metric.datatype = datatype
    last = session["last_value"].get(alias)
    if last is None:
        metric.is_null = True
        metric.timestamp = ts_ms
        return
    value, value_ts, is_null = last
    metric.timestamp = value_ts
    if is_null:
        metric.is_null = True
    else:
        set_metric_value(metric, datatype, value)


def build_nbirth(Payload, session: dict, model: dict, bdseq: int, ts_ms: int):
    """NBIRTH: bdSeq, Node Control/Rebirth, and every declared node metric."""
    payload = Payload()
    payload.timestamp = ts_ms
    payload.seq = 0
    bdseq_metric = payload.metrics.add()
    bdseq_metric.name = "bdSeq"
    bdseq_metric.datatype = _DT_INT64
    bdseq_metric.timestamp = ts_ms
    bdseq_metric.long_value = bdseq & 0xFFFFFFFFFFFFFFFF
    rebirth = payload.metrics.add()
    rebirth.name = "Node Control/Rebirth"
    rebirth.datatype = _BOOLEAN_TYPE
    rebirth.timestamp = ts_ms
    rebirth.boolean_value = False
    for spec in model["node_metrics"]:
        alias = alias_for(session, "", spec["name"])
        _birth_metric(payload, session, spec["name"], alias, spec["datatype"], ts_ms)
    return payload


def build_dbirth(Payload, session: dict, model: dict, device: str, table: str, ts_ms: int):
    """DBIRTH for one device: its table's field metrics with their last values."""
    payload = Payload()
    payload.timestamp = ts_ms
    for spec in model["field_specs_by_table"].get(table, []):
        alias = alias_for(session, device, spec["name"])
        _birth_metric(payload, session, spec["name"], alias, spec["datatype"], ts_ms)
    return payload


def build_data(Payload, session: dict, device: str, metrics: list, ts_ms: int, live_window_ms: int):
    """NDATA/DDATA: metrics resolved to their aliases, with timestamp and is_historical.

    Buffered metrics carry ``(name, datatype, value, ts)``, so the alias is resolved
    here against the current session — a backlog survives an alias reset (finding 3).
    Per ``tck-id-payloads-metric-datatype-not-req`` a DATA metric carries only the
    alias, no name and no datatype, so the message stays compact.
    """
    payload = Payload()
    payload.timestamp = ts_ms
    for name, datatype, value, metric_ts_ms in metrics:
        alias = alias_for(session, device, name)  # takes session["lock"]
        metric = payload.metrics.add()
        metric.alias = alias
        metric.timestamp = metric_ts_ms
        if live_window_ms and metric_ts_ms < ts_ms - live_window_ms:
            metric.is_historical = True
        set_metric_value(metric, datatype, value)
    return payload


def build_ndeath(Payload, bdseq: int, ts_ms: int):
    """NDEATH: the pending bdSeq only, so it matches the following NBIRTH."""
    payload = Payload()
    payload.timestamp = ts_ms
    bdseq_metric = payload.metrics.add()
    bdseq_metric.name = "bdSeq"
    bdseq_metric.datatype = _DT_INT64
    bdseq_metric.timestamp = ts_ms
    bdseq_metric.long_value = bdseq & 0xFFFFFFFFFFFFFFFF
    return payload


def build_ddeath(Payload, ts_ms: int):
    """DDEATH for a device that timed out; it carries no metrics."""
    payload = Payload()
    payload.timestamp = ts_ms
    return payload


def _message_size(payload, topic: str) -> int:
    """Approximate wire size of a message: payload, topic and a fixed overhead."""
    return len(payload.SerializeToString()) + len(topic.encode("utf-8")) + _MESSAGE_OVERHEAD_BYTES


def check_birth_sizes(Payload, cfg: dict, model: dict) -> None:
    """Refuse a birth larger than max_payload_bytes before connecting (A-4).

    The birth cannot be split or dropped, so an oversized one is a configuration
    error rather than a reconnect storm. A scratch session keeps the check from
    polluting the real alias table.
    """
    limit = cfg["max_payload_bytes"]
    scratch = {"aliases": {}, "next_alias": 1, "last_value": {}, "lock": threading.Lock()}
    ts = now_ms()

    nbirth = build_nbirth(Payload, scratch, model, 0, ts)
    size = _message_size(nbirth, nbirth_topic(cfg))
    if size > limit:
        raise ValueError(f"NBIRTH is {size} bytes, over max_payload_bytes ({limit})")

    for table, specs in model["field_specs_by_table"].items():
        if not specs:
            continue
        device = "sample" if model["mode"] == "device_tag" else table
        dbirth = build_dbirth(Payload, scratch, model, device, table, ts)
        size = _message_size(dbirth, dbirth_topic(cfg, device))
        if size > limit:
            raise ValueError(
                f"DBIRTH for table {table!r} is {size} bytes, over max_payload_bytes ({limit})"
            )


def parse_ncmd_rebirth(Payload, raw: bytes) -> bool:
    """True when an NCMD payload asks for a rebirth.

    Reads ``Node Control/Rebirth``'s boolean directly and ignores ``datatype``,
    which the specification says SHOULD NOT appear on an NCMD metric (S-1).
    """
    payload = Payload()
    payload.ParseFromString(raw)
    for metric in payload.metrics:
        if metric.name == "Node Control/Rebirth" and metric.boolean_value:
            return True
    return False


def parse_state(raw: bytes) -> dict:
    """Parse a STATE message: plain JSON {'online': bool, 'timestamp': ms}."""
    data = json.loads(raw.decode("utf-8"))
    return {"online": bool(data.get("online")), "timestamp": int(data.get("timestamp", 0))}


# --- client and callbacks ---------------------------------------------------

RC_SESSION_TAKEN_OVER = 0x8E


def _rc_value(reason_code):
    """Numeric value of a paho ReasonCode via ``.value``; int() on it raises (no __int__)."""
    if reason_code is None:
        return None
    value = getattr(reason_code, "value", None)
    if value is not None:
        return value
    try:
        return int(reason_code)
    except (TypeError, ValueError):
        return None


def _reason_is_failure(reason_code) -> bool:
    """True for a SUBACK/CONNACK reason code that denotes a failure (>= 0x80)."""
    if getattr(reason_code, "is_failure", None) is not None:
        return bool(reason_code.is_failure)
    value = _rc_value(reason_code)
    return value is not None and value >= 0x80


def _server_disconnect_reason(client):
    """Reason byte of the server DISCONNECT being handled; None if unreadable.

    paho 2.1 decodes it only past two bytes, so mosquitto's reason-only
    ``e0 01 8e`` reaches on_disconnect as 0 while the packet is still buffered.
    """
    try:
        packet = client._in_packet
        if (packet["command"] & 0xF0) != mqtt.DISCONNECT:
            return None
        return packet["packet"][0] if packet["remaining_length"] else 0
    except (AttributeError, KeyError, IndexError, TypeError):
        return None


# HARD RULE (§2): these callbacks run on paho's network thread. They must never
# touch influxdb3_local, publish, or take a plugin lock — they write only to the
# plain structures in the state (flags, deques). The one exception is
# on_pre_connect, which takes send_lock (A-2), safe because paho holds no mutex
# of its own there.


def on_pre_connect(client, userdata):
    """Before every CONNECT: bump bdSeq and the epoch, and set the matching will."""
    state = userdata
    with state["send_lock"]:
        session = state["session"]
        session["bdseq"] = (session["bdseq"] + 1) & 0xFF  # 0-255, seeded at -1 (S-4)
        state["epoch"] += 1
        bdseq = session["bdseq"]
    ndeath = build_ndeath(state["Payload"], bdseq, now_ms())
    client.will_set(ndeath_topic(state["cfg"]), ndeath.SerializeToString(), qos=1, retain=False)


def on_connect(client, userdata, flags, reason_code, properties=None):
    """CONNACK: flag it and wake the sender to run the on-connect sequence (S-2)."""
    state = userdata
    if _reason_is_failure(reason_code):
        state["events"].append(("warn", f"CONNACK refused: {reason_code}"))
        return
    # MQTT 5 may declare a smaller Maximum Packet Size than configured (§7.1).
    if properties is not None:
        maximum = getattr(properties, "MaximumPacketSize", None)
        state["broker_max_packet"] = int(maximum) if maximum else None
    if state["last_connect_monotonic"]:
        state["counters"]["reconnects"] += 1
    state["flags"]["connected"] = True
    state["flags"]["disconnected"] = False
    state["last_connect_monotonic"] = time.monotonic()
    state["wakeup"].set()


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    """Disconnect: clear connected and session_ready, record the reason, wake sender (2.3)."""
    state = userdata
    reason = _rc_value(reason_code)  # .value; int() on a ReasonCode raises
    if reason == 0 and disconnect_flags.is_disconnect_packet_from_server:
        reason = _server_disconnect_reason(client)
    state["last_disconnect_rc"] = reason  # before disconnect_pending, which the sender polls
    state["flags"]["connected"] = False
    state["flags"]["disconnected"] = True
    state["flags"]["session_ready"] = False
    state["disconnect_pending"] = True
    state["wakeup"].set()


def on_publish(client, userdata, mid, reason_code=None, properties=None):
    """Record a message id the broker has taken from us (QoS 0: socket write)."""
    userdata["confirmations"].append(mid)


def on_message(client, userdata, message):
    """Buffer a raw inbound NCMD/STATE message; the sender decodes it (S-1)."""
    state = userdata
    state["commands"].append((message.topic, bytes(message.payload)))
    state["wakeup"].set()


def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
    """Check the SUBACK: a denied subscription means no rebirth/STATE (3.7)."""
    state = userdata
    for reason_code in reason_code_list:
        if _reason_is_failure(reason_code):
            state["events"].append(
                ("error", f"subscription denied by broker (reason {reason_code})")
            )


def _tls_context(cfg: dict) -> ssl.SSLContext:
    """Build the TLS context from the configured CA, client cert and options."""
    context = ssl.create_default_context(
        cafile=str(resolve_path(cfg["tls_ca_cert"])) if cfg["tls_ca_cert"] else None
    )
    if cfg["tls_client_cert"]:
        context.load_cert_chain(
            str(resolve_path(cfg["tls_client_cert"])),
            str(resolve_path(cfg["tls_client_key"])),
        )
    if cfg["tls_insecure_skip_verify"]:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    if cfg["tls_alpn"]:
        context.set_alpn_protocols([cfg["tls_alpn"]])
    return context


def _install_socket_timeout(client, keep_alive: int) -> None:
    """Shorten half-open detection with TCP_USER_TIMEOUT where available (3.6)."""
    user_timeout = getattr(socket, "TCP_USER_TIMEOUT", None)
    if user_timeout is None:
        return

    def on_socket_open(cl, userdata, sock):
        try:
            sock.setsockopt(socket.IPPROTO_TCP, user_timeout, int(keep_alive * 1500))
        except OSError:
            pass

    client.on_socket_open = on_socket_open


def build_client(state: dict):
    """Build, configure and start the paho client for the current configuration."""
    cfg = state["cfg"]
    broker = parse_broker_url(cfg["broker_url"])
    protocol = mqtt.MQTTv5 if cfg["mqtt_version"] == "5" else mqtt.MQTTv311

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=cfg["client_id"],
        protocol=protocol,
        transport=broker["transport"],
        clean_session=None if protocol == mqtt.MQTTv5 else True,
    )
    client.user_data_set(state)
    client.suppress_exceptions = True  # second defence for the hard rule (S-1)
    client.reconnect_delay_set(
        min_delay=int(cfg["reconnect_min_backoff"]),
        max_delay=int(cfg["reconnect_max_backoff"]),
    )
    if cfg["username"]:
        client.username_pw_set(cfg["username"], cfg["password"] or None)
    if broker["tls"]:
        client.tls_set_context(_tls_context(cfg))
    if broker["transport"] == "websockets":
        client.ws_set_options(path=cfg["ws_path"] or broker["path"], headers=cfg["ws_headers"] or None)
    _install_socket_timeout(client, cfg["keep_alive"])

    client.on_pre_connect = on_pre_connect
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_publish = on_publish
    client.on_message = on_message
    client.on_subscribe = on_subscribe

    # MQTT 5 needs Clean Start true on every connect, not paho's first-only
    # default (1.4); 3.1.1 carries clean_session set at construction.
    connect_kwargs = {"keepalive": int(cfg["keep_alive"])}
    if protocol == mqtt.MQTTv5:
        connect_kwargs["clean_start"] = True
    client.connect_async(broker["host"], broker["port"], **connect_kwargs)
    client.loop_start()
    return client


# --- sender thread ----------------------------------------------------------

SENDER_TICK_SECONDS = 1.0


def _device_table(state: dict, device: str) -> str:
    """The source table a device belongs to."""
    if state["model"]["mode"] == "device_table":
        return device
    return state["cfg"]["source_measurements"][0]  # device_tag: exactly one table


def _entry_size(entry: dict) -> int:
    """Python footprint charged to a buffered entry (D-1)."""
    return BUFFER_ENTRY_OVERHEAD + BUFFER_METRIC_OVERHEAD * len(entry["metrics"])


def _effective_payload_limit(state: dict) -> int:
    """max_payload_bytes clamped to the broker's Maximum Packet Size (§7.1)."""
    limit = state["cfg"]["max_payload_bytes"]
    broker = state.get("broker_max_packet")
    return min(limit, broker) if broker else limit


def _log(state: dict, level: str, text: str) -> None:
    """Queue a log line for the next invocation to emit (hard rule, §2)."""
    state["events"].append((level, text))


def _reconcile(state: dict) -> None:
    """Clear confirmed message ids, keeping the unconfirmed byte total honest."""
    confirmations = state["confirmations"]
    unconfirmed = state["unconfirmed"]
    while confirmations:
        mid = confirmations.popleft()
        size = unconfirmed.pop(mid, 0)
        state["unconfirmed_bytes"] -= size
        state["counters"]["confirmed"] += 1
    if state["unconfirmed_bytes"] < 0:
        state["unconfirmed_bytes"] = 0


def _raw_publish(state: dict, topic: str, payload, qos: int = 0) -> bool:
    """Publish one message, tracking it as unconfirmed; False if the socket refused."""
    data = payload.SerializeToString()
    info = state["client"].publish(topic, data, qos=qos)
    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        return False
    size = len(data) + len(topic.encode("utf-8")) + _MESSAGE_OVERHEAD_BYTES
    state["unconfirmed"][info.mid] = size
    state["unconfirmed_bytes"] += size
    return True


def _rate_limit(state: dict) -> None:
    """Token-bucket pacing; waits on wakeup rather than sleeping (2.3, 7.2)."""
    rate = state["cfg"]["max_messages_per_second"]
    if not rate:
        return
    now = time.monotonic()
    state["rate_tokens"] = min(
        float(rate), state["rate_tokens"] + (now - state["rate_last"]) * rate
    )
    state["rate_last"] = now
    if state["rate_tokens"] >= 1.0:
        state["rate_tokens"] -= 1.0
        return
    wait = (1.0 - state["rate_tokens"]) / rate
    # Wait on a dedicated event, not the shared wakeup: a write, command or connect
    # must not shortcut the pacing, and the shared wakeup must not be swallowed here;
    # only a teardown sets rate_wakeup to break it early (finding 10).
    state["rate_wakeup"].wait(timeout=wait)
    state["rate_wakeup"].clear()
    state["rate_tokens"] = 0.0


def _request_rebirth(state: dict) -> None:
    """Note a rebirth request, coalescing a burst into one sequence (4.2)."""
    if not state["flags"]["rebirth_requested"]:
        state["flags"]["rebirth_requested"] = True
        state["rebirth_due_at"] = time.monotonic() + REBIRTH_COALESCE_SECONDS


def _handle_state(state: dict, status: dict) -> None:
    """Apply a STATE message, ignoring one that predates the last online (1.6)."""
    if status["timestamp"] < state["last_state_online_ts"]:
        return
    if status["online"]:
        state["last_state_online_ts"] = status["timestamp"]
        state["primary_online"] = True
    else:
        state["primary_online"] = False
        state["offline_requested"] = True


def _decode_commands(state: dict) -> None:
    """Drain buffered NCMD/STATE messages and act on them (S-1, 5.6)."""
    commands = state["commands"]
    while commands:
        topic, payload = commands.popleft()
        if "/NCMD/" in topic:
            try:
                if parse_ncmd_rebirth(state["Payload"], payload):
                    _request_rebirth(state)
            except Exception as exc:
                state["counters"]["command_errors"] += 1
                _log(state, "warn", f"NCMD decode failed: {describe_error(exc, state['cfg']['enable_full_logging'])}")
        elif "/STATE/" in topic:
            try:
                _handle_state(state, parse_state(payload))
            except Exception as exc:
                state["counters"]["command_errors"] += 1
                _log(state, "warn", f"STATE decode failed: {describe_error(exc, state['cfg']['enable_full_logging'])}")


def _publish_birth_sequence(state: dict) -> bool:
    """Publish NBIRTH then a DBIRTH for every known device (eager, 1.1).

    Bound to the connection it starts on: if a reconnect bumps the epoch mid-way,
    it abandons the sequence so no birth or data is split across sessions (finding 11).
    """
    cfg, session, model, payload_cls = state["cfg"], state["session"], state["model"], state["Payload"]
    birth_epoch = state["epoch"]
    ts = now_ms()
    nbirth = build_nbirth(payload_cls, session, model, session["bdseq"], ts)
    with state["send_lock"]:
        if state["epoch"] != birth_epoch or not _raw_publish(state, nbirth_topic(cfg), nbirth, qos=0):
            return False
        session["seq"] = 1
        state["ready_epoch"] = birth_epoch  # readiness is for this connection only
    for device in list(session["known_devices"]):
        dbirth = build_dbirth(payload_cls, session, model, device, _device_table(state, device), ts)
        _rate_limit(state)
        with state["send_lock"]:
            if state["epoch"] != birth_epoch:
                return False
            dbirth.seq = session["seq"]
            if not _raw_publish(state, dbirth_topic(cfg, device), dbirth, qos=0):
                return False
            session["seq"] = (session["seq"] + 1) & 0xFF
    state["counters"]["births"] += 1
    state["birth_published_at"] = time.monotonic()
    return True


def _establish_session(state: dict) -> None:
    """On-connect sequence: subscribe, wait for STATE online, publish the birth (S-2)."""
    cfg, client = state["cfg"], state["client"]
    if state["subscribed_epoch"] != state["epoch"]:
        client.subscribe(ncmd_topic(cfg), qos=1)
        if cfg["sparkplug_primary_host_id"]:
            client.subscribe(state_topic(cfg), qos=1)
        state["subscribed_epoch"] = state["epoch"]
    if cfg["sparkplug_primary_host_id"] and not state["primary_online"]:
        return  # hold NBIRTH until the primary host is online (5.6)
    if _publish_birth_sequence(state):
        state["flags"]["session_ready"] = True  # ready_epoch set inside the sequence (finding 11)
        _log(state, "info", "session ready")


def _serve_rebirth(state: dict) -> None:
    """Answer a coalesced rebirth with the complete birth sequence (1.1, 4.2)."""
    if time.monotonic() < state["rebirth_due_at"]:
        return
    state["flags"]["rebirth_requested"] = False
    if not state["flags"]["connected"]:
        return
    if state["cfg"]["sparkplug_primary_host_id"] and not state["primary_online"]:
        return  # hold the rebirth until the primary host is online (5.6, finding 13)
    state["counters"]["rebirths_served"] += 1
    if not _publish_birth_sequence(state):
        state["flags"]["session_ready"] = False


def _maybe_offline(state: dict) -> None:
    """On STATE offline: NDEATH and a clean stop; the next invocation rebuilds (5.6, 2.5)."""
    if not state.get("offline_requested"):
        return
    state["offline_requested"] = False
    # Nothing was birthed this session — we were holding NBIRTH until the primary
    # host came online — so there is no session to tear down; keep waiting (finding 1).
    if not state["flags"]["session_ready"]:
        return
    cfg, session = state["cfg"], state["session"]
    state["flags"]["session_ready"] = False
    with state["send_lock"]:
        _raw_publish(state, ndeath_topic(cfg), build_ndeath(state["Payload"], session["bdseq"], now_ms()), qos=1)
    # Stop here and let obtain_state rebuild on the invocation thread; reconnecting
    # from the sender thread races paho's loop (finding 2). The rebuild subscribes
    # to STATE again and holds NBIRTH until the primary host is back online.
    state["stop_reason"] = "primary_offline"
    _stop_client(state)
    _log(state, "info", "primary host offline; stopping, will rebuild and await STATE online")


def _device_timeout_sweep(state: dict) -> None:
    """DDEATH for a device idle longer than sparkplug_device_timeout (5.5)."""
    timeout = state["cfg"]["sparkplug_device_timeout"]
    if not timeout or not state["flags"]["session_ready"]:
        return
    session, cfg = state["session"], state["cfg"]
    now = time.monotonic()
    for device, last_seen in list(session["known_devices"].items()):
        if now - last_seen < timeout:
            continue
        with state["send_lock"]:
            if state["epoch"] != state["ready_epoch"]:
                return  # stale session; the sweep retries after re-establish (finding 11)
            ddeath = build_ddeath(state["Payload"], now_ms())
            ddeath.seq = session["seq"]
            if _raw_publish(state, ddeath_topic(cfg, device), ddeath, qos=0):
                session["seq"] = (session["seq"] + 1) & 0xFF
        del session["known_devices"][device]
        _log(state, "info", f"device {device!r} timed out, published DDEATH")


def _ensure_device_birth(state: dict, device: str) -> bool:
    """Publish a DBIRTH for a newly seen device before its first DDATA (lazy for new ones)."""
    session = state["session"]
    if device == "" or device in session["known_devices"]:
        if device != "":
            session["known_devices"][device] = time.monotonic()
        return True
    dbirth = build_dbirth(state["Payload"], session, state["model"], device, _device_table(state, device), now_ms())
    with state["send_lock"]:
        if state["epoch"] != state["ready_epoch"]:
            return False  # stale session; requeue and wait for re-establish (finding 11)
        dbirth.seq = session["seq"]
        if not _raw_publish(state, dbirth_topic(state["cfg"], device), dbirth, qos=0):
            return False
        session["seq"] = (session["seq"] + 1) & 0xFF
    session["known_devices"][device] = time.monotonic()
    return True


def _pop_entry(state: dict):
    """Next entry to publish; live_first prefers a recent one over the backlog (1.5)."""
    pending = state["pending"]
    if not pending:
        return None
    if state["cfg"]["drain_order"] != "live_first":
        return pending.popleft()
    live_window_ms = int(state["cfg"]["live_window"] * 1000)
    threshold = now_ms() - live_window_ms if live_window_ms else None
    if threshold is not None:
        for index, entry in enumerate(pending):
            if entry["max_ts"] >= threshold:
                del pending[index]
                return entry
    return pending.popleft()


def _flush_chunk(state: dict, topic: str, device: str, metrics: list, live_window_ms: int, now: int) -> bool:
    """Publish one data message, re-gating under send_lock (A-2); False to requeue (3.5)."""
    _rate_limit(state)  # paced outside the lock so on_pre_connect is never blocked
    session = state["session"]
    with state["send_lock"]:
        if not state["flags"]["session_ready"] or state["epoch"] != state["ready_epoch"]:
            return False
        payload = build_data(state["Payload"], session, device, metrics, now, live_window_ms)
        payload.seq = session["seq"]
        if not _raw_publish(state, topic, payload, qos=0):
            return False  # NO_CONN: seq not advanced, caller requeues (3.5)
        session["seq"] = (session["seq"] + 1) & 0xFF
    state["counters"]["published"] += 1
    return True


def _publish_entry(state: dict, entry: dict):
    """Publish an entry, splitting it to stay under max_payload_bytes (§7).

    Returns the metrics not yet sent (to requeue) if a send is refused, else None.
    A single metric too large for the limit is dropped, counted and logged.
    """
    cfg, session, payload_cls = state["cfg"], state["session"], state["Payload"]
    live_window_ms = int(cfg["live_window"] * 1000)
    limit = _effective_payload_limit(state)
    device = entry["device"]
    topic = ndata_topic(cfg) if device == "" else ddata_topic(cfg, device)
    overhead = len(topic.encode("utf-8")) + _MESSAGE_OVERHEAD_BYTES
    metrics = entry["metrics"]
    # One "now" for the whole entry, so the discard cutoff below and the
    # is_historical marking in build_data judge every metric against the same
    # instant — no boundary metric can be both kept and marked historical.
    now = now_ms()
    # §7: with sparkplug_replay_historical=false, a metric old enough to be marked
    # is_historical is discarded instead, so no is_historical=true message is sent.
    if not cfg["sparkplug_replay_historical"] and live_window_ms:
        cutoff = now - live_window_ms
        kept = [metric for metric in metrics if metric[3] >= cutoff]
        state["counters"]["dropped_historical"] += len(metrics) - len(kept)
        metrics = kept
        if not metrics:
            return None
    chunk: list = []
    index = 0
    while index < len(metrics):
        metric = metrics[index]
        try:
            size = len(build_data(payload_cls, session, device, chunk + [metric], now, live_window_ms).SerializeToString())
        except Exception as exc:
            state["counters"]["dropped_unencodable"] += 1
            _log(state, "warn", f"encode failed: {describe_error(exc, cfg['enable_full_logging'])}")
            index += 1
            continue
        if size + overhead <= limit:
            chunk.append(metric)
            index += 1
            continue
        if chunk:
            if not _flush_chunk(state, topic, device, chunk, live_window_ms, now):
                return chunk + metrics[index:]
            chunk = []
            continue  # retry this metric at the head of a fresh chunk
        # the metric alone does not fit
        state["counters"]["dropped_unencodable"] += 1
        _log(state, "warn", "a single metric exceeds max_payload_bytes; dropped")
        index += 1
    if chunk and not _flush_chunk(state, topic, device, chunk, live_window_ms, now):
        return chunk
    return None


def _drain_pending(state: dict) -> None:
    """Publish buffered data; split per entry, re-gate per message (§7, A-2, 3.5)."""
    cfg = state["cfg"]
    lock = state["session"]["lock"]
    max_age_ms = int(cfg["max_message_age"] * 1000)
    while state["pending"]:
        if not state["flags"]["session_ready"]:
            return
        if state["unconfirmed_bytes"] > cfg["max_unconfirmed_bytes"]:
            return
        with lock:
            entry = _pop_entry(state)
            if entry is None:
                return
            state["pending_bytes"] -= _entry_size(entry)
        if max_age_ms and entry["max_ts"] < now_ms() - max_age_ms:
            state["counters"]["dropped_age"] += len(entry["metrics"])
            continue
        if not _ensure_device_birth(state, entry["device"]):
            with lock:
                state["pending"].appendleft(entry)
                state["pending_bytes"] += _entry_size(entry)
            return
        remaining = _publish_entry(state, entry)
        if remaining:
            requeued = {
                "device": entry["device"], "table": entry["table"],
                "metrics": remaining, "max_ts": entry["max_ts"],
            }
            with lock:
                state["pending"].appendleft(requeued)
                state["pending_bytes"] += _entry_size(requeued)
            return


def _watchdog(state: dict) -> None:
    """Reap an idle client, publishing NDEATH first, once its buffer is empty (4.5)."""
    idle = time.monotonic() - state["last_call"]
    if idle < state["cfg"]["idle_disconnect"]:
        return
    if state["pending"] and idle < 2 * state["cfg"]["idle_disconnect"]:
        return  # keep trying to drain a backlog for one more idle_disconnect
    if state["pending"]:
        state["counters"]["dropped_shutdown"] += sum(len(e["metrics"]) for e in state["pending"])
        _log(state, "info", f"watchdog discarding {len(state['pending'])} buffered entries")
    try:
        with state["send_lock"]:
            _raw_publish(state, ndeath_topic(state["cfg"]), build_ndeath(state["Payload"], state["session"]["bdseq"], now_ms()), qos=1)
        state["client"].disconnect()
        state["client"].loop_stop()
    except Exception:
        pass
    state["stop"] = True


def _stop_client(state: dict) -> None:
    """Stop reconnecting and end the threads; the invocation rebuilds per stop_reason (2.2)."""
    try:
        state["client"].disconnect()
        state["client"].loop_stop()
    except Exception:
        pass
    state["stopped_at"] = time.monotonic()
    state["stop"] = True


def _handle_disconnect(state: dict) -> None:
    """Tell a takeover or a refused birth from an ordinary drop (§4.6, 2.1, A-4).

    An ordinary drop is left to paho's automatic reconnect; only a takeover or a
    birth the broker refuses stops the client, with a stop_reason the invocation
    reads before rebuilding (2.2). MQTT 5 is exact; 3.1.1, and an MQTT 5 server
    DISCONNECT whose reason could not be read, fall back to a best-effort heuristic.
    """
    if not state.get("disconnect_pending"):
        return
    state["disconnect_pending"] = False
    # QoS 0 messages handed over but not yet confirmed are lost with the socket.
    state["counters"]["lost"] += len(state["unconfirmed"])
    state["unconfirmed"].clear()
    state["unconfirmed_bytes"] = 0
    cfg = state["cfg"]
    reason_code = state.get("last_disconnect_rc")
    duration = time.monotonic() - state.get("last_connect_monotonic", 0.0)
    since_birth = time.monotonic() - state.get("birth_published_at", 0.0)

    if cfg["mqtt_version"] == "5" and reason_code is not None:
        if reason_code in (0x95, 0x87):  # packet too large / not authorized
            state["stop_reason"] = "birth_rejected"
            _log(state, "error", f"birth refused by broker (reason {reason_code}); stopping")
            _stop_client(state)
        elif reason_code == RC_SESSION_TAKEN_OVER:
            state["stop_reason"] = "taken_over"
            _log(state, "warn", "session taken over (0x8E); stopping")
            _stop_client(state)
        else:
            state["quick_close_streak"] = 0
        return

    # No reason code (3.1.1, or an unreadable MQTT 5 one). A close right after our
    # own birth is a refused birth, not a takeover (A-4); otherwise count quick
    # closes toward the takeover guess.
    if duration < 5.0:
        if state.get("birth_published_at") and since_birth < 5.0:
            state["stop_reason"] = "birth_rejected"
            _log(state, "error", "birth refused or session taken over (3.1.1 cannot distinguish); backing off")
            _stop_client(state)
            return
        state["quick_close_streak"] = state.get("quick_close_streak", 0) + 1
        if state["quick_close_streak"] >= cfg["takeover_attempts"]:
            state["stop_reason"] = "taken_over"
            _log(state, "warn", "superseded by another client (3.1.1 heuristic); stopping")
            _stop_client(state)
    else:
        state["quick_close_streak"] = 0


def sender_loop(state: dict) -> None:
    """The single publisher and the watchdog (§4.4)."""
    while not state["stop"]:
        state["wakeup"].wait(timeout=SENDER_TICK_SECONDS)
        state["wakeup"].clear()
        try:
            _reconcile(state)
            _handle_disconnect(state)       # takeover / refused birth vs ordinary drop (§4.6)
            _maybe_offline(state)          # STATE offline: sender owns the reconnect (2.5)
            _decode_commands(state)         # NCMD/STATE, off the network thread (S-1)
            if state["flags"]["connected"] and not state["flags"]["session_ready"]:
                _establish_session(state)   # resubscribe + birth on every connect (S-2)
            if state["flags"]["rebirth_requested"]:
                _serve_rebirth(state)       # complete birth sequence (1.1), coalesced (4.2)
            _device_timeout_sweep(state)
            if state["flags"]["session_ready"]:
                _drain_pending(state)
            _watchdog(state)
        except Exception as exc:
            # The sender must not die on an unexpected error (A-6); log and carry on.
            state["counters"]["sender_errors"] += 1
            _log(state, "error", f"sender: {describe_error(exc, state['cfg']['enable_full_logging'])}")


# --- state and lifecycle ----------------------------------------------------

# Settings whose change rebuilds the client, vs those that only republish the
# birth in the same session (§4.4).
CONNECTION_KEYS = (
    "broker_url", "client_id", "mqtt_version", "username", "password", "keep_alive",
    "tls_ca_cert", "tls_client_cert", "tls_client_key", "tls_insecure_skip_verify",
    "tls_alpn", "ws_path", "ws_headers", "reconnect_min_backoff", "reconnect_max_backoff",
    "takeover_attempts", "sparkplug_group_id", "sparkplug_edge_node_id",
    "sparkplug_primary_host_id",
)
BIRTH_KEYS = ("payload_fields", "sparkplug_device_from", "source_measurements")

# A change to any of these abandons the old node's topics or broker, so the old
# session is sent an NDEATH before teardown rather than left online on the host.
IDENTITY_KEYS = ("sparkplug_group_id", "sparkplug_edge_node_id", "broker_url")

COUNTER_KEYS = (
    "published", "confirmed", "buffered", "dropped_policy", "dropped_age",
    "dropped_historical", "dropped_unencodable", "dropped_shutdown", "lost",
    "reconnects", "rebirths_served", "births", "command_errors", "sender_errors",
)

TAKEOVER_COOLDOWN_SECONDS = 10.0
BIRTH_REJECTED_COOLDOWN_SECONDS = 300.0

_MISSING = object()


def _fingerprint(cfg: dict, keys) -> str:
    return hashlib.sha1(repr([cfg[key] for key in keys]).encode()).hexdigest()


def connection_fingerprint(cfg: dict) -> str:
    return _fingerprint(cfg, CONNECTION_KEYS)


def birth_fingerprint(cfg: dict) -> str:
    return _fingerprint(cfg, BIRTH_KEYS)


def new_state(cfg: dict, payload_cls, model: dict) -> dict:
    """A fresh cached-state dict of plain containers (never isinstance-checked, §2, §16)."""
    now = time.monotonic()
    return {
        "cfg": cfg,
        "fingerprint": connection_fingerprint(cfg),
        "birth_fingerprint": birth_fingerprint(cfg),
        "Payload": payload_cls,
        "model": model,
        "client": None,
        "sender": None,
        "session": {
            "bdseq": -1,  # seeded at -1 so the first CONNECT carries 0 (S-4)
            "seq": 0,
            "aliases": {},
            "next_alias": 1,
            "last_value": {},
            "known_devices": {},
            # Guards the encoding state and the buffer against the sender thread,
            # since ingest and apply_birth_change run on the invocation thread.
            "lock": threading.Lock(),
        },
        "pending": deque(),
        "pending_bytes": 0,
        "unconfirmed": {},
        "unconfirmed_bytes": 0,
        "confirmations": deque(),
        "commands": deque(),
        "events": deque(),
        "flags": {
            "connected": False,
            "disconnected": False,
            "session_ready": False,
            "reconnecting": False,
            "rebirth_requested": False,
        },
        "counters": {key: 0 for key in COUNTER_KEYS},
        "last_call": now,
        "wakeup": threading.Event(),
        "rate_wakeup": threading.Event(),  # pacing wait only, never set by callbacks (finding 10)
        "send_lock": threading.Lock(),
        "stop": False,
        "stop_reason": None,
        "stopped_at": 0.0,
        "epoch": 0,
        "primary_online": not bool(cfg["sparkplug_primary_host_id"]),
        "last_state_online_ts": 0,
        "offline_requested": False,
        "disconnect_pending": False,
        "quick_close_streak": 0,
        "subscribed_epoch": -1,
        "ready_epoch": -1,
        "rebirth_due_at": 0.0,
        "rate_tokens": 0.0,
        "rate_last": now,
        "last_disconnect_rc": None,
        "last_connect_monotonic": 0.0,
        "birth_published_at": 0.0,
        "broker_max_packet": None,
        "last_stats_at": now,
    }


def build_state(influxdb3_local, cfg: dict) -> dict:
    """Build the metric model, validate the births, and start the client and sender."""
    payload_cls = build_payload_class()
    model = build_metric_model(influxdb3_local, cfg)  # reads schema; checks tags (3.2)
    check_birth_sizes(payload_cls, cfg, model)  # refuse an oversized birth (A-4)
    state = new_state(cfg, payload_cls, model)
    state["client"] = build_client(state)
    sender = threading.Thread(target=sender_loop, args=(state,), daemon=True)
    state["sender"] = sender
    sender.start()
    return state


def teardown_state(state: dict, send_death: bool = False) -> None:
    """Stop a client and its threads with a clean DISCONNECT (the will is suppressed).

    With ``send_death`` and a session that was ready, publish NDEATH on the old
    topics first — used when the node's identity or broker changes, so the host is
    not left showing a dead node online indefinitely (finding 4).
    """
    state["stop"] = True
    state["wakeup"].set()
    state["rate_wakeup"].set()  # break a pacing wait the sender may be in (finding 10)
    client = state.get("client")
    if client is None:
        return
    try:
        if send_death and state["flags"]["session_ready"]:
            session = state["session"]
            with state["send_lock"]:
                _raw_publish(
                    state, ndeath_topic(state["cfg"]),
                    build_ndeath(state["Payload"], session["bdseq"], now_ms()), qos=1,
                )
        client.disconnect()
        client.loop_stop()
    except Exception:
        pass


def _cooldown_elapsed(state: dict) -> bool:
    """Whether a deliberate stop's cooldown has passed, so a rebuild may proceed (2.2)."""
    reason = state.get("stop_reason")
    if reason == "birth_rejected":
        cooldown = BIRTH_REJECTED_COOLDOWN_SECONDS
    elif reason == "taken_over":
        cooldown = TAKEOVER_COOLDOWN_SECONDS
    else:
        return True
    return time.monotonic() - state.get("stopped_at", 0.0) >= cooldown


def _detach_buffer(state: dict) -> dict:
    """Keep the backlog and known devices to carry into a rebuilt client (3.4)."""
    return {
        "pending": state["pending"],
        "pending_bytes": state["pending_bytes"],
        "known_devices": dict(state["session"]["known_devices"]),
    }


def _rebuild(influxdb3_local, cfg: dict, carry: dict | None) -> dict:
    """Build a fresh client, carrying the backlog when the metric set is unchanged."""
    state = build_state(influxdb3_local, cfg)
    if carry is not None:
        state["pending"] = carry["pending"]
        state["pending_bytes"] = carry["pending_bytes"]
        state["session"]["known_devices"] = carry["known_devices"]
    return state


def obtain_state(influxdb3_local, cfg: dict, task_id: str):
    """Return the one live client for this node, building or rebuilding as needed."""
    cache = influxdb3_local.cache
    state = cache.get(STATE_CACHE_KEY)

    if state is not None:
        dead = state["stop"] or not state["sender"].is_alive()
        if dead and not _cooldown_elapsed(state):
            # Surface the stop reason before going quiet, or the cooldown is a
            # silent data gap in the log (finding 8).
            drain_events(influxdb3_local, state, task_id)
            return None  # a deliberate stop is still cooling down (2.2)
        rebuild = dead or connection_fingerprint(cfg) != state["fingerprint"]
        if not rebuild:
            return state
        # Carry the backlog only when the metric set is unchanged (§4.4, 3.4b).
        carry = _detach_buffer(state) if birth_fingerprint(cfg) == state["birth_fingerprint"] else None
        identity_changed = any(state["cfg"][key] != cfg[key] for key in IDENTITY_KEYS)
        teardown_state(state, send_death=identity_changed)
        state = _rebuild(influxdb3_local, cfg, carry)
    else:
        state = build_state(influxdb3_local, cfg)

    cache.put(STATE_CACHE_KEY, state, None)
    # A synchronous trigger makes the cold-cache race impossible (§4.4); this
    # read-back is a cheap defence against another client sneaking in.
    current = cache.get(STATE_CACHE_KEY)
    if current is not state:
        teardown_state(state)
        return current
    return state


def apply_birth_change(influxdb3_local, state: dict, cfg: dict) -> None:
    """Rebuild the metric model and rebirth when the metric set changed (§4.4, L-4)."""
    new_model = build_metric_model(influxdb3_local, cfg)
    check_birth_sizes(state["Payload"], cfg, new_model)
    mapping_changed = (
        state["cfg"]["sparkplug_device_from"] != cfg["sparkplug_device_from"]
        or state["cfg"]["source_measurements"] != cfg["source_measurements"]
    )
    state["model"] = new_model
    state["birth_fingerprint"] = birth_fingerprint(cfg)
    session = state["session"]
    with session["lock"]:
        session["aliases"] = {}
        session["next_alias"] = 1
        session["last_value"] = {}
        if mapping_changed:
            # Forget the old mapping's devices so stale ones are not re-announced (L-4).
            session["known_devices"] = {}
    state["flags"]["rebirth_requested"] = True
    state["rebirth_due_at"] = time.monotonic()


# --- ingest -----------------------------------------------------------------


def _compare(actual, operator: str, expected) -> bool:
    """Evaluate one filter comparison, returning False on an incomparable pair."""
    if operator == "!=":
        return actual != expected
    if operator == "=":
        return actual == expected
    try:
        if operator == ">=":
            return actual >= expected
        if operator == "<=":
            return actual <= expected
        if operator == ">":
            return actual > expected
        if operator == "<":
            return actual < expected
    except TypeError:
        return False
    return False


def _row_passes_filters(cfg: dict, row: dict) -> bool:
    """Apply filters and allow/deny membership tests to one row (§6)."""
    for column, operator, expected in cfg["filters"]:
        actual = row.get(column, _MISSING)
        if actual is _MISSING:
            if operator != "!=":
                return False
            continue
        if not _compare(actual, operator, expected):
            return False
    for column, allowed in cfg["allow_values"].items():
        if str(row.get(column)) not in allowed:
            return False
    for column, denied in cfg["deny_values"].items():
        if str(row.get(column)) in denied:
            return False
    return True


def _row_device(state: dict, table: str, row: dict):
    """The device a row maps to: '' for node level, the id for a device, None to skip."""
    model = state["model"]
    if model["mode"] == "node":
        return ""
    if model["mode"] == "device_table":
        value = table
    else:  # device_tag
        value = row.get(model["device_tag"])
        if value in (None, ""):
            return None  # a row without the device tag is skipped (3.8)
        value = str(value)
    if reserved_char(value) is not None or value.startswith("$"):
        return None  # a reserved character would make a malformed device topic
    return value


def _value_changed(session: dict, alias: int, value, deadband: float) -> bool:
    """Whether a value differs from the last published for report_by_exception (1.2)."""
    last = session["last_value"].get(alias)
    if last is None:
        return True
    previous, _, previous_null = last
    if (value is None) != previous_null:
        return True
    if value is None:
        return False
    if (
        isinstance(value, (int, float))
        and isinstance(previous, (int, float))
        and not isinstance(value, bool)
    ):
        return abs(value - previous) > deadband
    return value != previous


def _buffer_entry(state: dict, entry: dict) -> None:
    """Append an entry to the buffer, enforcing max_queue_bytes by the drop policy."""
    cfg = state["cfg"]
    with state["session"]["lock"]:
        state["pending"].append(entry)
        state["pending_bytes"] += _entry_size(entry)
        state["counters"]["buffered"] += len(entry["metrics"])
        while state["pending_bytes"] > cfg["max_queue_bytes"] and state["pending"]:
            dropped = state["pending"].pop() if cfg["drop_policy"] == "drop_newest" else state["pending"].popleft()
            state["pending_bytes"] -= _entry_size(dropped)
            state["counters"]["dropped_policy"] += len(dropped["metrics"])


def _unresolved_fields(cfg: dict, model: dict) -> set:
    """Declared fields (no type override) not yet in the model — unknown type at build (D-3)."""
    declared = {
        field for field, override, _unit in (split_field_type(e) for e in cfg["payload_fields"]) if override is None
    }
    if model["mode"] == "node":
        resolved = {spec["field"] for spec in model["node_metrics"]}
    else:
        resolved = {spec["field"] for specs in model["field_specs_by_table"].values() for spec in specs}
    return declared - resolved


def _batches_have_fields(table_batches: list, sources: set, fields: set) -> bool:
    """Whether any source-table row carries one of the given fields."""
    for batch in table_batches:
        if batch["table_name"] not in sources:
            continue
        for row in batch["rows"]:
            if fields & set(row):
                return True
    return False


def ingest(influxdb3_local, state: dict, table_batches: list) -> None:
    """Turn WAL rows into compact buffered entries for the sender (§6, 1.2, 3.8)."""
    cfg, session = state["cfg"], state["session"]
    sources = set(cfg["source_measurements"])

    # D-3: a declared field with no type at build rejoins when it first appears,
    # via one schema re-read, and the new metric triggers a rebirth.
    unresolved = _unresolved_fields(cfg, state["model"])
    if unresolved and _batches_have_fields(table_batches, sources, unresolved):
        new_model = build_metric_model(influxdb3_local, cfg, refresh=True)
        if _unresolved_fields(cfg, new_model) != unresolved:
            state["model"] = new_model
            state["flags"]["rebirth_requested"] = True
            state["rebirth_due_at"] = time.monotonic()
            influxdb3_local.info("schema re-read resolved new metrics; rebirth scheduled")

    model = state["model"]
    for batch in table_batches:
        table = batch["table_name"]
        if table not in sources:
            continue
        groups: dict = {}
        for row in batch["rows"]:
            device = _row_device(state, table, row)
            if device is None or not _row_passes_filters(cfg, row):
                continue
            ts_ms = ms_from_ns(row["time"])
            specs = (
                [spec for spec in model["node_metrics"] if spec["table"] == table]
                if model["mode"] == "node"
                else model["field_specs_by_table"].get(table, [])
            )
            for spec in specs:
                if spec["field"] not in row:
                    continue
                value = row[spec["field"]]
                if spec["datetime_unit"] and value is not None:
                    value = datetime_to_ms(value, spec["datetime_unit"])
                alias = alias_for(session, device, spec["name"])
                if cfg["report_by_exception"] and not _value_changed(session, alias, value, cfg["deadband"]):
                    continue
                # An out-of-order (older) row is still published, but must not
                # regress the birth's last value to a stale reading.
                previous = session["last_value"].get(alias)
                if previous is None or ts_ms >= previous[1]:
                    session["last_value"][alias] = (value, ts_ms, value is None)
                group = groups.setdefault(device, {"metrics": [], "max_ts": 0})
                # Store name+datatype, not the alias, so the entry survives an
                # alias reset across a rebuild (finding 3); alias is resolved on drain.
                group["metrics"].append((spec["name"], spec["datatype"], value, ts_ms))
                group["max_ts"] = max(group["max_ts"], ts_ms)
        for device, group in groups.items():
            if group["metrics"]:
                _buffer_entry(
                    state,
                    {"device": device, "table": table, "metrics": group["metrics"], "max_ts": group["max_ts"]},
                )


# --- observability ----------------------------------------------------------


def drain_events(influxdb3_local, state: dict, task_id: str) -> None:
    """Emit the log lines the callbacks and sender queued (§2, §8)."""
    events = state["events"]
    while events:
        level, text = events.popleft()
        getattr(influxdb3_local, level, influxdb3_local.info)(f"[{task_id}] {text}")


def write_stats(influxdb3_local, state: dict) -> None:
    """Write counters to the stats measurement and summarise them, per stats_interval."""
    interval = state["cfg"]["stats_interval"]
    now = time.monotonic()
    if now - state["last_stats_at"] < interval:
        return
    state["last_stats_at"] = now
    counters = state["counters"]
    summary = " ".join(f"{key}={value}" for key, value in counters.items())
    influxdb3_local.info(
        f"sparkplug stats queue={len(state['pending'])}/{state['pending_bytes']}B "
        f"devices={len(state['session']['known_devices'])} connected={state['flags']['connected']} {summary}"
    )
    if interval <= 0:
        return
    broker = parse_broker_url(state["cfg"]["broker_url"])
    line = (
        LineBuilder(STATS_MEASUREMENT)
        .tag("broker", f"{broker['host']}:{broker['port']}")
        .tag("group", state["cfg"]["sparkplug_group_id"])
        .tag("edge_node", state["cfg"]["sparkplug_edge_node_id"])
    )
    for key, value in counters.items():
        line.int64_field(key, int(value))
    line.int64_field("queue_depth", len(state["pending"]))
    line.int64_field("queue_bytes", int(state["pending_bytes"]))
    line.int64_field("devices_known", len(state["session"]["known_devices"]))
    line.time_ns(time.time_ns())
    influxdb3_local.write(line)


# --- entry point ------------------------------------------------------------


def process_writes(influxdb3_local, table_batches: list, args: dict | None = None):
    """WAL entry point: queue rows for the background sender to publish."""
    args = args or {}
    task_id = uuid.uuid4().hex[:8]

    try:
        cfg = load_configuration(influxdb3_local, args, task_id)
    except Exception as exc:
        influxdb3_local.error(f"[{task_id}] configuration: {describe_error(exc, True)}")
        return

    try:
        state = obtain_state(influxdb3_local, cfg, task_id)
        if state is None:
            return  # a deliberate stop is cooling down (2.2)
        if birth_fingerprint(cfg) != state["birth_fingerprint"]:
            apply_birth_change(influxdb3_local, state, cfg)
        state["cfg"] = cfg
        state["last_call"] = time.monotonic()
        drain_events(influxdb3_local, state, task_id)
        ingest(influxdb3_local, state, table_batches)
        state["wakeup"].set()
        write_stats(influxdb3_local, state)
    except Exception as exc:
        influxdb3_local.error(f"[{task_id}] {describe_error(exc, cfg['enable_full_logging'])}")
