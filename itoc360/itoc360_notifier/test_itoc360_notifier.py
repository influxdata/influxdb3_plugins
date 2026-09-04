import importlib.util
from pathlib import Path


PLUGIN_PATH = Path(__file__).with_name("itoc360_notifier.py")

spec = importlib.util.spec_from_file_location("itoc360_notifier", PLUGIN_PATH)
plugin = importlib.util.module_from_spec(spec)
spec.loader.exec_module(plugin)


def test_build_check_id_is_deterministic():
    first = plugin.build_check_id(
        "cpu_threshold",
        "cpu",
        {"region": "eu", "host": "server01"},
    )

    second = plugin.build_check_id(
        "cpu_threshold",
        "cpu",
        {"host": "server01", "region": "eu"},
    )

    assert first == second
    assert first == "cpu_threshold:cpu:host=server01,region=eu"


def test_check_id_does_not_include_runtime_values():
    check_id = plugin.build_check_id(
        "cpu_threshold",
        "cpu",
        {"host": "server01", "region": "eu"},
    )

    assert "95" not in check_id
    assert "2026" not in check_id


def test_resolve_level_greater_than():
    assert plugin.resolve_level(95, "gt", 90, 75) == plugin.LEVEL_CRIT
    assert plugin.resolve_level(80, "gt", 90, 75) == plugin.LEVEL_WARN
    assert plugin.resolve_level(50, "gt", 90, 75) == plugin.LEVEL_OK


def test_resolve_level_less_than():
    assert plugin.resolve_level(5, "lt", 10, 20) == plugin.LEVEL_CRIT
    assert plugin.resolve_level(15, "lt", 10, 20) == plugin.LEVEL_WARN
    assert plugin.resolve_level(30, "lt", 10, 20) == plugin.LEVEL_OK


def test_build_payload_matches_itoc360_contract():
    payload = plugin.build_payload(
        check_id="cpu_threshold:cpu:host=server01,region=eu",
        check_name="CPU Threshold",
        level="crit",
        message="CPU threshold breached",
        measurement="cpu",
    )

    assert payload["_check_id"] == "cpu_threshold:cpu:host=server01,region=eu"
    assert payload["_check_name"] == "CPU Threshold"
    assert payload["_type"] == "threshold"
    assert payload["_level"] == "crit"
    assert payload["_message"] == "CPU threshold breached"
    assert payload["_source_measurement"] == "cpu"
    assert "_time" in payload


def test_redact_url_removes_token():
    url = (
        "https://api.itoc360.app/functions/v1/events"
        "?token=super-secret-token"
    )

    redacted = plugin.redact_url(url)

    assert "super-secret-token" not in redacted
    assert redacted.endswith("?token=***")


def test_parse_window():
    assert plugin.parse_window("30s") == 30
    assert plugin.parse_window("5min") == 300
    assert plugin.parse_window("2h") == 7200
    assert plugin.parse_window("1d") == 86400
