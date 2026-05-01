import os
import sys
import tempfile
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

SKIPPED = []


def _try_import(module_path):
    """Try importing a module; return (module, None) on success or (None, reason) on failure."""
    try:
        import importlib
        mod = importlib.import_module(module_path)
        return mod, None
    except ImportError as e:
        return None, str(e)


class MockCache:
    def __init__(self):
        self._data = {}

    def get(self, key):
        return self._data.get(key)

    def put(self, key, value):
        self._data[key] = value

    def delete(self, key):
        self._data.pop(key, None)


class MockInfluxDB3Local:
    def __init__(self):
        self.logs = []
        self.cache = MockCache()

    def info(self, msg):
        self.logs.append(("info", msg))

    def warn(self, msg):
        self.logs.append(("warn", msg))

    def error(self, msg):
        self.logs.append(("error", msg))


def _logs_contain_path(logs, path):
    for _, msg in logs:
        if path in msg:
            return True
    return False


def _find_log(logs, substring):
    for _, msg in logs:
        if substring in msg:
            return msg
    return None


def test_mqtt_missing_config_no_path_leak():
    """Missing config file: exception and logs must not contain the resolved path."""
    mqtt_mod, reason = _try_import("influxdata.mqtt_subscriber.mqtt_subscriber")
    if mqtt_mod is None:
        SKIPPED.append(f"test_mqtt_missing_config_no_path_leak ({reason})")
        return
    MQTTConfig = mqtt_mod.MQTTConfig

    mock = MockInfluxDB3Local()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.environ["PLUGIN_DIR"] = tmpdir
        caught = None
        try:
            MQTTConfig(mock, {"config_file_path": "nonexistent.toml"}, "task-1")
        except Exception as e:
            caught = e
        assert caught is not None, "Expected an exception for missing config"
        assert tmpdir not in str(caught), (
            f"Exception leaked temp path {tmpdir}: {caught}"
        )
        assert not _logs_contain_path(mock.logs, tmpdir), (
            f"Leaked temp path {tmpdir} in logs: {mock.logs}"
        )


def test_mqtt_unreadable_config_no_path_leak():
    """Unreadable config file: exception and logs must not contain the resolved path."""
    mqtt_mod, reason = _try_import("influxdata.mqtt_subscriber.mqtt_subscriber")
    if mqtt_mod is None:
        SKIPPED.append(f"test_mqtt_unreadable_config_no_path_leak ({reason})")
        return
    if os.getuid() == 0:
        SKIPPED.append("test_mqtt_unreadable_config_no_path_leak (running as root)")
        return
    MQTTConfig = mqtt_mod.MQTTConfig

    mock = MockInfluxDB3Local()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.environ["PLUGIN_DIR"] = tmpdir
        config_file = os.path.join(tmpdir, "test.toml")
        with open(config_file, "w") as f:
            f.write('[mqtt]\nbroker = "localhost"\ntopics = ["test"]\n')
        os.chmod(config_file, 0o000)
        caught = None
        try:
            MQTTConfig(mock, {"config_file_path": "test.toml"}, "task-2")
        except Exception as e:
            caught = e
        finally:
            os.chmod(config_file, 0o644)
        assert caught is not None, "Expected an exception for unreadable config"
        assert tmpdir not in str(caught), (
            f"Exception leaked temp path {tmpdir}: {caught}"
        )
        assert not _logs_contain_path(mock.logs, tmpdir), (
            f"Leaked temp path {tmpdir} in logs: {mock.logs}"
        )


def test_mqtt_missing_cert_no_path_leak():
    """Missing TLS cert: connect() error log must not contain the resolved path,
    and must contain the expected sanitized error message proving _configure_tls ran."""
    mqtt_mod, reason = _try_import("influxdata.mqtt_subscriber.mqtt_subscriber")
    if mqtt_mod is None:
        SKIPPED.append(f"test_mqtt_missing_cert_no_path_leak ({reason})")
        return
    MQTTConnectionManager = mqtt_mod.MQTTConnectionManager

    mock = MockInfluxDB3Local()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.environ["PLUGIN_DIR"] = tmpdir
        config = {
            "broker_host": "localhost",
            "broker_port": 1883,
            "tls": {"ca_cert": "nonexistent_ca.pem"},
        }
        mgr = MQTTConnectionManager(config, mock, "task-3")
        result = mgr.connect()
        assert result is False, "Expected connect() to return False"
        assert not _logs_contain_path(mock.logs, tmpdir), (
            f"Leaked temp path {tmpdir} in logs: {mock.logs}"
        )
        cert_error_log = _find_log(mock.logs, "TLS configuration failed")
        assert cert_error_log is not None, (
            f"Expected a log containing the sanitized TLS error, "
            f"proving _configure_tls ran. Got: {mock.logs}"
        )


def test_kafka_connect_handler_suppresses_paths():
    """Kafka connect() error handler must not log cert paths when SSL is configured.
    Uses a real temp cert file so os.path.exists passes in _build_consumer_config,
    then monkeypatches Consumer to raise an error containing the cert path.
    Asserts Consumer was called (proving we reached that code path) and that
    the logged message is the generic SSL failure, not str(e)."""
    kafka_mod, reason = _try_import("influxdata.kafka_subscriber.kafka_subscriber")
    if kafka_mod is None:
        SKIPPED.append(f"test_kafka_connect_handler_suppresses_paths ({reason})")
        return
    KafkaConsumerManager = kafka_mod.KafkaConsumerManager

    mock = MockInfluxDB3Local()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.environ["PLUGIN_DIR"] = tmpdir
        ca_file = os.path.join(tmpdir, "ca.pem")
        with open(ca_file, "w") as f:
            f.write("fake cert")

        config = {
            "bootstrap_servers": "localhost:9092",
            "topics": ["test"],
            "group_id": "test-group",
            "security_protocol": "SSL",
            "ssl": {"ca_cert": ca_file},
        }
        mgr = KafkaConsumerManager(config, mock, "task-4")

        mock_consumer_cls = MagicMock(
            side_effect=Exception(
                f"SSL certificate problem: unable to get local issuer certificate "
                f"(ca location: {ca_file})"
            )
        )
        with patch(
            "influxdata.kafka_subscriber.kafka_subscriber.Consumer",
            mock_consumer_cls,
        ):
            result = mgr.connect()

        assert mock_consumer_cls.called, (
            "Consumer() was never called — os.path.exists check may have blocked execution"
        )
        assert result is False, "Expected connect() to return False"
        assert not _logs_contain_path(mock.logs, tmpdir), (
            f"Leaked cert path {tmpdir} in logs: {mock.logs}"
        )
        assert not _logs_contain_path(mock.logs, ca_file), (
            f"Leaked cert file path {ca_file} in logs: {mock.logs}"
        )
        error_log = _find_log(mock.logs, "SSL/TLS configured")
        assert error_log is not None, (
            f"Expected the generic SSL error log, got: {mock.logs}"
        )


if __name__ == "__main__":
    passed = 0
    failed = 0
    for name, func in sorted(globals().items()):
        if name.startswith("test_") and callable(func):
            try:
                func()
                print(f"  PASS: {name}")
                passed += 1
            except Exception as e:
                print(f"  FAIL: {name}: {e}")
                failed += 1
    for skip_msg in SKIPPED:
        print(f"  SKIP: {skip_msg}")
    print(f"\n{passed} passed, {failed} failed, {len(SKIPPED)} skipped")
    sys.exit(1 if failed else 0)
