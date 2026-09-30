"""Tests for import.py functions."""

import json
import os
from datetime import datetime, timedelta, timezone

import pytest
from unittest.mock import Mock, patch

# Note: The module is named "import" which is a Python keyword
# We need to use importlib to import it
import builtins
import importlib

builtins.LineBuilder = getattr(builtins, "LineBuilder", object)
import_module = importlib.import_module("import")

_parse_url_with_port_inference = import_module._parse_url_with_port_inference
_validate_test_connection_params = import_module._validate_test_connection_params
check_source_connection = import_module.check_source_connection
_build_v3_headers = import_module._build_v3_headers
_parse_v3_databases = import_module._parse_v3_databases
_parse_v3_tables = import_module._parse_v3_tables
_validate_source_params = import_module._validate_source_params
get_source_databases_list = import_module.get_source_databases_list
get_source_tables_list = import_module.get_source_tables_list
query_source_influxdb = import_module.query_source_influxdb
ImportConfig = import_module.ImportConfig
load_import_settings = import_module.load_import_settings
check_query_result = import_module.check_query_result
count_rows_in_result = import_module.count_rows_in_result
format_nanoseconds_iso = import_module.format_nanoseconds_iso
parse_timestamp_to_nanoseconds = import_module.parse_timestamp_to_nanoseconds
write_to_destination = import_module.write_to_destination
SourceQueryError = import_module.SourceQueryError


class FakeLocal:
    """The runtime surface the configuration and write paths touch."""

    def __init__(self, fail_write=None):
        self.infos, self.errors, self.writes = [], [], []
        self.fail_write = fail_write

    def info(self, message):
        self.infos.append(message)

    def warn(self, message):
        pass

    def error(self, message):
        self.errors.append(message)

    def write_sync(self, payload, no_sync=None):
        self._record(None, payload)

    def write_sync_to_db(self, database, payload, no_sync=None):
        self._record(database, payload)

    def _record(self, database, payload):
        if self.fail_write:
            raise RuntimeError(self.fail_write)
        self.writes.append((database, payload.build()))


class FakeBuilder:
    def __init__(self, line):
        self.line = line

    def build(self):
        return self.line


@pytest.fixture
def clean_environment(monkeypatch, tmp_path):
    """No inherited variables, and a plugin directory for relative paths."""
    for name in list(os.environ):
        if name.startswith(("IMPORT_", "INFLUXDB3_IMPORT_")):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PLUGIN_DIR", str(tmp_path))
    monkeypatch.delenv("INFLUXDB3_PLUGIN_DIR", raising=False)
    return monkeypatch


REQUIRED_ARGS = {
    "source_url": "http://localhost:8086",
    "source_database": "telegraf",
    "influxdb_version": "1",
}


def load(args=None, body=None, headers=None, query=None):
    return load_import_settings(
        FakeLocal(),
        "task",
        args or {},
        json.dumps(body) if body is not None else None,
        headers,
        query,
    )


def call_plugin(query, headers=None, body=None, args=None):
    return import_module.process_request(
        FakeLocal(), query, headers or {}, body, args or {}
    )


class TestParseUrlWithPortInference:
    """Tests for _parse_url_with_port_inference."""

    def test_url_with_explicit_port_unchanged(self):
        result = _parse_url_with_port_inference("http://localhost:8086")
        assert result == "http://localhost:8086"

    def test_http_url_infers_port_80(self):
        result = _parse_url_with_port_inference("http://localhost")
        assert result == "http://localhost:80"

    def test_https_url_infers_port_443(self):
        result = _parse_url_with_port_inference("https://localhost")
        assert result == "https://localhost:443"

    def test_url_with_path_preserved(self):
        result = _parse_url_with_port_inference("http://localhost:8086/api")
        assert result == "http://localhost:8086/api"

    def test_trailing_slash_removed(self):
        result = _parse_url_with_port_inference("http://localhost:8086/")
        assert result == "http://localhost:8086"

    def test_https_with_explicit_port(self):
        result = _parse_url_with_port_inference("https://myserver.com:9999")
        assert result == "https://myserver.com:9999"


class TestValidateTestConnectionParams:
    """Tests for _validate_test_connection_params."""

    def test_valid_source_url_returns_none(self):
        result = _validate_test_connection_params({"source_url": "http://localhost:8086"})
        assert result is None

    def test_missing_source_url_returns_error(self):
        result = _validate_test_connection_params({})
        assert result == {"message": "source_url is required"}

    def test_empty_source_url_returns_error(self):
        result = _validate_test_connection_params({"source_url": ""})
        assert result == {"message": "source_url is required"}

    def test_whitespace_source_url_returns_error(self):
        result = _validate_test_connection_params({"source_url": "   "})
        assert result == {"message": "source_url is required"}

    def test_none_source_url_returns_error(self):
        result = _validate_test_connection_params({"source_url": None})
        assert result == {"message": "source_url is required"}


class TestCheckSourceConnection:
    """Tests for check_source_connection."""

    def test_influxdb_detected_returns_success_with_version_build(self):
        mock_session = Mock()
        mock_response = Mock()
        mock_response.headers = {
            "X-Influxdb-Version": "2.7.0",
            "X-Influxdb-Build": "OSS",
        }
        mock_session.get.return_value = mock_response

        result = check_source_connection(
            {"source_url": "http://localhost:8086"},
            session=mock_session,
        )

        assert result == {"success": True, "version": "2.7.0", "build": "OSS"}
        mock_session.get.assert_called_once()

    def test_no_influxdb_headers_returns_failure(self):
        mock_session = Mock()
        mock_response = Mock()
        mock_response.headers = {}
        mock_session.get.return_value = mock_response

        result = check_source_connection(
            {"source_url": "http://localhost:8086"},
            session=mock_session,
        )

        assert result == {"success": False, "message": "Not an InfluxDB instance"}

    def test_only_version_header_returns_success(self):
        mock_session = Mock()
        mock_response = Mock()
        mock_response.headers = {"X-Influxdb-Version": "1.8.10"}
        mock_session.get.return_value = mock_response

        result = check_source_connection(
            {"source_url": "http://localhost:8086"},
            session=mock_session,
        )

        assert result == {"success": True, "version": "1.8.10", "build": ""}

    def test_only_build_header_returns_success(self):
        mock_session = Mock()
        mock_response = Mock()
        mock_response.headers = {"X-Influxdb-Build": "Enterprise"}
        mock_session.get.return_value = mock_response

        result = check_source_connection(
            {"source_url": "http://localhost:8086"},
            session=mock_session,
        )

        assert result == {"success": True, "version": "", "build": "Enterprise"}

    def test_request_exception_returns_failure_with_raw_message(self):
        import requests

        mock_session = Mock()
        mock_session.get.side_effect = requests.exceptions.ConnectionError(
            "HTTPConnectionPool(host='localhost', port=8086): Max retries exceeded"
        )

        result = check_source_connection(
            {"source_url": "http://localhost:8086"},
            session=mock_session,
        )

        assert result["success"] is False
        assert "Max retries exceeded" in result["message"]

    def test_timeout_returns_failure_with_raw_message(self):
        import requests

        mock_session = Mock()
        mock_session.get.side_effect = requests.exceptions.Timeout("Read timed out")

        result = check_source_connection(
            {"source_url": "http://localhost:8086"},
            session=mock_session,
        )

        assert result["success"] is False
        assert "timed out" in result["message"]

    def test_missing_source_url_returns_validation_error(self):
        result = check_source_connection({})

        assert result == {"success": False, "message": "source_url is required"}

    def test_port_inferred_from_http_scheme(self):
        mock_session = Mock()
        mock_response = Mock()
        mock_response.headers = {"X-Influxdb-Version": "2.0.0", "X-Influxdb-Build": "OSS"}
        mock_session.get.return_value = mock_response

        check_source_connection(
            {"source_url": "http://localhost"},
            session=mock_session,
        )

        call_url = mock_session.get.call_args[0][0]
        assert call_url == "http://localhost:80/ping"

    def test_port_inferred_from_https_scheme(self):
        mock_session = Mock()
        mock_response = Mock()
        mock_response.headers = {"X-Influxdb-Version": "2.0.0", "X-Influxdb-Build": "OSS"}
        mock_session.get.return_value = mock_response

        check_source_connection(
            {"source_url": "https://myserver.com"},
            session=mock_session,
        )

        call_url = mock_session.get.call_args[0][0]
        assert call_url == "https://myserver.com:443/ping"

    def test_cluster_uuid_header_detects_v3(self):
        mock_session = Mock()
        mock_response = Mock()
        mock_response.headers = {"cluster-uuid": "8a66b257-af97-41c1-a3a8-3c04b7451ebd"}
        mock_session.get.return_value = mock_response

        result = check_source_connection(
            {"source_url": "http://localhost:8086"},
            session=mock_session,
        )

        assert result == {"success": True, "version": "3.x.x", "build": ""}

    def test_version_headers_take_precedence_over_cluster_uuid(self):
        mock_session = Mock()
        mock_response = Mock()
        mock_response.headers = {
            "X-Influxdb-Version": "2.7.0",
            "X-Influxdb-Build": "OSS",
            "cluster-uuid": "8a66b257-af97-41c1-a3a8-3c04b7451ebd",
        }
        mock_session.get.return_value = mock_response

        result = check_source_connection(
            {"source_url": "http://localhost:8086"},
            session=mock_session,
        )

        assert result == {"success": True, "version": "2.7.0", "build": "OSS"}

    def test_401_without_headers_returns_unable_to_determine(self):
        mock_session = Mock()
        mock_response = Mock()
        mock_response.headers = {}
        mock_response.status_code = 401
        mock_session.get.return_value = mock_response

        result = check_source_connection(
            {"source_url": "http://localhost:8086"},
            session=mock_session,
        )

        assert result == {"success": False, "message": "Unable to determine InfluxDB version"}

    def test_403_without_headers_returns_unable_to_determine(self):
        mock_session = Mock()
        mock_response = Mock()
        mock_response.headers = {}
        mock_response.status_code = 403
        mock_session.get.return_value = mock_response

        result = check_source_connection(
            {"source_url": "http://localhost:8086"},
            session=mock_session,
        )

        assert result == {"success": False, "message": "Unable to determine InfluxDB version"}

    def test_401_with_version_headers_returns_success(self):
        mock_session = Mock()
        mock_response = Mock()
        mock_response.headers = {"X-Influxdb-Version": "2.7.0", "X-Influxdb-Build": "OSS"}
        mock_response.status_code = 401
        mock_session.get.return_value = mock_response

        result = check_source_connection(
            {"source_url": "http://localhost:8086"},
            session=mock_session,
        )

        assert result == {"success": True, "version": "2.7.0", "build": "OSS"}


class TestBuildV3Headers:
    """Tests for _build_v3_headers."""

    def test_with_token(self):
        credentials = {"source_token": "my-token", "source_username": None, "source_password": None}
        headers = _build_v3_headers(credentials)
        assert headers == {
            "Content-Type": "application/json",
            "Authorization": "Bearer my-token",
        }

    def test_without_token(self):
        credentials = {"source_token": None, "source_username": None, "source_password": None}
        headers = _build_v3_headers(credentials)
        assert headers == {"Content-Type": "application/json"}


class TestParseV3Databases:
    """Tests for _parse_v3_databases."""

    def test_extracts_database_names(self):
        result = _parse_v3_databases([
            {"iox::database": "_internal"},
            {"iox::database": "import"},
            {"iox::database": "test"},
        ])
        assert result == ["import", "test"]

    def test_filters_internal_database(self):
        result = _parse_v3_databases([
            {"iox::database": "_internal"},
            {"iox::database": "mydb"},
        ])
        assert "_internal" not in result
        assert result == ["mydb"]

    def test_empty_response_returns_empty_list(self):
        result = _parse_v3_databases([])
        assert result == []


class TestParseV3Tables:
    """Tests for _parse_v3_tables."""

    def test_extracts_iox_schema_tables(self):
        result = _parse_v3_tables([
            {"table_catalog": "public", "table_schema": "iox", "table_name": "import_pause_state", "table_type": "BASE TABLE"},
            {"table_catalog": "public", "table_schema": "system", "table_name": "compacted_data", "table_type": "BASE TABLE"},
            {"table_catalog": "public", "table_schema": "information_schema", "table_name": "tables", "table_type": "VIEW"},
        ])
        assert result == ["import_pause_state"]

    def test_filters_system_schema(self):
        result = _parse_v3_tables([
            {"table_catalog": "public", "table_schema": "system", "table_name": "queries", "table_type": "BASE TABLE"},
        ])
        assert result == []

    def test_filters_information_schema(self):
        result = _parse_v3_tables([
            {"table_catalog": "public", "table_schema": "information_schema", "table_name": "columns", "table_type": "VIEW"},
        ])
        assert result == []

    def test_empty_response_returns_empty_list(self):
        result = _parse_v3_tables([])
        assert result == []


class TestValidateSourceParams:
    """Tests for _validate_source_params."""

    def test_version_3_is_valid(self):
        result = _validate_source_params({
            "source_url": "http://localhost:8086",
            "influxdb_version": 3,
        })
        assert result is None

    def test_version_0_is_invalid(self):
        result = _validate_source_params({
            "source_url": "http://localhost:8086",
            "influxdb_version": 0,
        })
        assert result == {"error": "influxdb_version must be one of (1, 2, 3), got 0"}

    def test_version_4_is_invalid(self):
        result = _validate_source_params({
            "source_url": "http://localhost:8086",
            "influxdb_version": 4,
        })
        assert result == {"error": "influxdb_version must be one of (1, 2, 3), got 4"}

    def test_non_numeric_string_version_is_invalid(self):
        result = _validate_source_params({
            "source_url": "http://localhost:8086",
            "influxdb_version": "abc",
        })
        assert result == {"error": "influxdb_version: Invalid integer: 'abc'"}

    def test_numeric_string_version_is_coerced_to_int(self):
        """Numeric strings like '3' should be accepted and coerced to int."""
        body_data = {
            "source_url": "http://localhost:8086",
            "influxdb_version": "3",
        }
        result = _validate_source_params(body_data)
        assert result is None  # Valid
        assert body_data["influxdb_version"] == 3  # Coerced to int


class TestGetSourceDatabasesListV3:
    """Tests for get_source_databases_list v3 support."""

    def test_v3_returns_databases_filtering_internal(self):
        mock_session = Mock()
        mock_response = Mock()
        mock_response.json.return_value = [
            {"iox::database": "_internal"},
            {"iox::database": "import"},
            {"iox::database": "test"},
        ]
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response

        result = get_source_databases_list(
            {
                "source_url": "http://localhost:8086",
                "influxdb_version": 3,
            },
            credentials={
                "source_token": "my-token",
                "source_username": None,
                "source_password": None,
            },
            session=mock_session,
        )

        assert result == {"databases": ["import", "test"]}
        mock_session.get.assert_called_once()
        call_args = mock_session.get.call_args
        assert "/api/v3/configure/database" in call_args[0][0]
        assert call_args[1]["params"] == {"format": "json"}


class TestGetSourceDatabasesListV2:
    """Tests for get_source_databases_list v2 support using InfluxQL."""

    def test_v2_returns_databases_using_influxql_endpoint(self):
        """Test that v2 uses /query endpoint with SHOW DATABASES, not /api/v2/buckets."""
        mock_session = Mock()
        mock_response = Mock()
        # InfluxQL response format (same as v1)
        mock_response.json.return_value = {
            "results": [
                {
                    "series": [
                        {
                            "name": "databases",
                            "columns": ["name"],
                            "values": [["mydb"], ["testdb"], ["_internal"]],
                        }
                    ]
                }
            ]
        }
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response

        result = get_source_databases_list(
            {
                "source_url": "http://localhost:8086",
                "influxdb_version": 2,
            },
            credentials={
                "source_token": "my-token",
                "source_username": None,
                "source_password": None,
            },
            session=mock_session,
        )

        # _internal should be filtered out
        assert result == {"databases": ["mydb", "testdb"]}
        mock_session.get.assert_called_once()
        call_args = mock_session.get.call_args
        # Verify it uses /query endpoint, not /api/v2/buckets
        assert "/query" in call_args[0][0]
        assert "/api/v2/buckets" not in call_args[0][0]
        assert call_args[1]["params"] == {"q": "SHOW DATABASES"}

    def test_v2_uses_token_auth_header(self):
        """Test that v2 uses Token authorization header."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.json.return_value = {"results": [{}]}
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response

        get_source_databases_list(
            {
                "source_url": "http://localhost:8086",
                "influxdb_version": 2,
            },
            credentials={
                "source_token": "my-secret-token",
                "source_username": None,
                "source_password": None,
            },
            session=mock_session,
        )

        call_args = mock_session.get.call_args
        headers = call_args[1]["headers"]
        assert headers.get("Authorization") == "Token my-secret-token"


class TestGetSourceTablesListV3:
    """Tests for get_source_tables_list v3 support."""

    def test_v3_returns_tables_filtering_system_schemas(self):
        mock_session = Mock()
        mock_response = Mock()
        mock_response.json.return_value = [
            {"table_catalog": "public", "table_schema": "iox", "table_name": "import_pause_state", "table_type": "BASE TABLE"},
            {"table_catalog": "public", "table_schema": "system", "table_name": "compacted_data", "table_type": "BASE TABLE"},
            {"table_catalog": "public", "table_schema": "information_schema", "table_name": "tables", "table_type": "VIEW"},
        ]
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response

        result = get_source_tables_list(
            {
                "source_url": "http://localhost:8086",
                "influxdb_version": 3,
                "source_database": "mydb",
            },
            credentials={
                "source_token": "my-token",
                "source_username": None,
                "source_password": None,
            },
            session=mock_session,
        )

        assert result == {"tables": ["import_pause_state"]}
        mock_session.get.assert_called_once()
        call_args = mock_session.get.call_args
        assert "/api/v3/query_sql" in call_args[0][0]
        assert call_args[1]["params"] == {"db": "mydb", "q": "SHOW TABLES", "format": "json"}


class TestGetSourceTablesListV2:
    """Tests for get_source_tables_list v2 support using InfluxQL."""

    def test_v2_returns_tables_using_influxql_endpoint(self):
        """Test that v2 uses /query endpoint with SHOW MEASUREMENTS, not Flux API."""
        mock_session = Mock()
        mock_response = Mock()
        # InfluxQL response format (same as v1)
        mock_response.json.return_value = {
            "results": [
                {
                    "series": [
                        {
                            "name": "measurements",
                            "columns": ["name"],
                            "values": [["cpu"], ["memory"], ["disk"]],
                        }
                    ]
                }
            ]
        }
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response

        result = get_source_tables_list(
            {
                "source_url": "http://localhost:8086",
                "influxdb_version": 2,
                "source_database": "mybucket",
                # Note: no source_org provided
            },
            credentials={
                "source_token": "my-token",
                "source_username": None,
                "source_password": None,
            },
            session=mock_session,
        )

        assert result == {"tables": ["cpu", "disk", "memory"]}
        mock_session.get.assert_called_once()
        call_args = mock_session.get.call_args
        # Verify it uses /query endpoint, not /api/v2/query
        assert "/query" in call_args[0][0]
        assert "/api/v2/query" not in call_args[0][0]
        assert call_args[1]["params"] == {"db": "mybucket", "q": "SHOW MEASUREMENTS"}

    def test_v2_does_not_require_org(self):
        """Test that v2 works without source_org parameter."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.json.return_value = {"results": [{"series": [{"values": [["test"]]}]}]}
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response

        # Should not return an error about missing org
        result = get_source_tables_list(
            {
                "source_url": "http://localhost:8086",
                "influxdb_version": 2,
                "source_database": "mybucket",
            },
            credentials={
                "source_token": "my-token",
                "source_username": None,
                "source_password": None,
            },
            session=mock_session,
        )

        assert "error" not in result
        assert "tables" in result

    def test_v2_uses_token_auth_header(self):
        """Test that v2 uses Token authorization header."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.json.return_value = {"results": [{}]}
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response

        get_source_tables_list(
            {
                "source_url": "http://localhost:8086",
                "influxdb_version": 2,
                "source_database": "mybucket",
            },
            credentials={
                "source_token": "my-secret-token",
                "source_username": None,
                "source_password": None,
            },
            session=mock_session,
        )

        call_args = mock_session.get.call_args
        headers = call_args[1]["headers"]
        assert headers.get("Authorization") == "Token my-secret-token"


class TestQuerySourceInfluxdbV3Auth:
    """Tests for query_source_influxdb v3 authentication."""

    @patch("import.get_http_session")
    def test_v3_uses_bearer_token_auth(self, mock_get_session):
        """Verify v3 uses Bearer token in Authorization header."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.json.return_value = {"results": [{"series": []}]}
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response
        mock_get_session.return_value = mock_session

        mock_influxdb3_local = Mock()

        # Create config WITHOUT credential fields
        config = ImportConfig(
            source_url="http://localhost",
            source_database="mydb",
            influxdb_version=3,
        )

        # Create credentials separately
        credentials = {
            "source_token": "my-v3-token",
            "source_username": None,
            "source_password": None,
        }

        # Pass credentials to function
        query_source_influxdb(mock_influxdb3_local, config, credentials, "SHOW MEASUREMENTS", "test-task")

        # Verify the Authorization header uses Bearer format
        call_kwargs = mock_session.get.call_args
        headers = call_kwargs.kwargs.get("headers", call_kwargs[1].get("headers", {}))
        assert headers.get("Authorization") == "Bearer my-v3-token"

    @patch("import.get_http_session")
    def test_v3_without_token_no_auth_header(self, mock_get_session):
        """Verify v3 without token results in no Authorization header."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.json.return_value = {"results": [{"series": []}]}
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response
        mock_get_session.return_value = mock_session

        mock_influxdb3_local = Mock()

        # Create config WITHOUT credential fields
        config = ImportConfig(
            source_url="http://localhost",
            source_database="mydb",
            influxdb_version=3,
        )

        # Create credentials with no token
        credentials = {
            "source_token": None,
            "source_username": None,
            "source_password": None,
        }

        # Pass credentials to function
        query_source_influxdb(mock_influxdb3_local, config, credentials, "SHOW MEASUREMENTS", "test-task")

        # Verify no Authorization header is present
        call_kwargs = mock_session.get.call_args
        headers = call_kwargs.kwargs.get("headers", call_kwargs[1].get("headers", {}))
        assert "Authorization" not in headers


def read_credentials(headers):
    """The credentials a request carries, as process_request reads them."""
    return import_module.parse_request_headers(
        headers, import_module.CREDENTIAL_HEADERS
    )


class TestCredentialHeaders:
    """Tests for the CREDENTIAL_HEADERS spec"""

    def test_extracts_token_from_headers(self):
        # a header that was not sent is absent, and every reader uses .get()
        assert read_credentials({"source-token": "my-secret-token"}) == {
            "source_token": "my-secret-token"
        }

    def test_extracts_username_password_from_headers(self):
        headers = {
            "source-username": "admin",
            "source-password": "secret123",
        }
        assert read_credentials(headers) == {
            "source_username": "admin",
            "source_password": "secret123",
        }

    def test_returns_nothing_for_missing_headers(self):
        assert read_credentials({}) == {}

    def test_a_header_the_plugin_did_not_ask_for_is_dropped(self):
        headers = {"user-agent": "curl", "host": "x", "Source-Token": "tok"}
        assert read_credentials(headers) == {"source_token": "tok"}

    def test_a_setting_header_is_not_read_as_a_credential(self):
        headers = {"X-Influxdb3-Import-Source-Url": "http://src", "Source-Token": "tok"}
        assert read_credentials(headers) == {"source_token": "tok"}

    def test_extracts_all_credentials_when_present(self):
        # InfluxDB3 normalizes headers to lowercase
        headers = {
            "source-token": "token",
            "source-username": "user",
            "source-password": "pass",
        }
        assert read_credentials(headers) == {
            "source_token": "token",
            "source_username": "user",
            "source_password": "pass",
        }


class TestActionQueryParameters:
    """Tests for QUERY_KEYS_BY_ACTION: an action accepts only what it reads."""

    def test_unknown_action_lists_the_available_ones(self):
        result = call_plugin({"action": "stat"})
        assert result["error"] == "Unknown action: stat"
        assert result["available_actions"] == list(import_module.QUERY_KEYS_BY_ACTION)

    @pytest.mark.parametrize(
        "query, refusal",
        [
            (
                {"action": "status", "import_ids": "abc"},
                "may not set 'import_ids'; accepted keys: ['action', 'import_id']",
            ),
            (
                {"action": "resume", "import_id": "i", "target_batch_size": "5000"},
                "may not set 'target_batch_size'; accepted keys: ['action', 'import_id']",
            ),
            (
                {"action": "databases", "source_url": "http://x"},
                "may not set 'source_url'; accepted keys: ['action']",
            ),
            ({"action": "start", "import_id": "x"}, "may not set 'import_id'"),
        ],
    )
    def test_a_parameter_the_action_does_not_read_is_refused(self, query, refusal):
        assert refusal in call_plugin(query)["error"]

    def test_an_absent_import_id_is_reported_as_before(self):
        assert call_plugin({"action": "status"})["error"] == "import_id required"

    @pytest.mark.parametrize("action", list(import_module.QUERY_KEYS_BY_ACTION))
    def test_every_declared_action_is_dispatched(self, action, clean_environment):
        # the dispatch has no fallback branch, so an action declared without one
        # would answer with None instead of a response
        assert isinstance(call_plugin({"action": action}), dict)

    def test_start_reads_its_settings_and_credentials_from_the_request(
        self, clean_environment
    ):
        captured = {}

        def fake_start(influxdb3_local, config, credentials, task_id):
            captured.update(config=config, credentials=credentials)
            return {"status": "started"}

        with patch.object(import_module, "start_import", fake_start):
            result = call_plugin(
                {"action": "start", **REQUIRED_ARGS, "target_batch_size": "5000"},
                {"Source-Token": "tok"},
            )

        # ImportConfig has no action field, so the action left in the settings
        # layer would have raised TypeError instead of loading
        assert result == {"status": "started"}
        # a query parameter arrives as text, so the validator cast has to run
        assert captured["config"].target_batch_size == 5000
        assert captured["config"].influxdb_version == 1
        assert captured["credentials"] == {"source_token": "tok"}


class TestConfigurationLayers:
    """Tests for load_import_settings: where each setting may come from."""

    def test_environment_is_the_lowest_layer(self, clean_environment):
        clean_environment.setenv("INFLUXDB3_IMPORT_SOURCE_URL", "http://from-env")
        clean_environment.setenv("INFLUXDB3_IMPORT_SOURCE_DATABASE", "telegraf")
        clean_environment.setenv("INFLUXDB3_IMPORT_INFLUXDB_VERSION", "2")
        clean_environment.setenv("INFLUXDB3_IMPORT_TARGET_BATCH_SIZE", "500")

        config = load()
        assert config.source_url == "http://from-env"
        assert config.influxdb_version == 2
        assert config.target_batch_size == 500

        # a trigger argument overrides the environment; an untouched variable stands
        overridden = load({"source_url": "http://from-args"})
        assert overridden.source_url == "http://from-args"
        assert overridden.target_batch_size == 500

    def test_new_environment_names_win_over_the_old_ones(self, clean_environment):
        clean_environment.setenv("IMPORT_SOURCE_URL", "http://old")
        clean_environment.setenv("IMPORT_SOURCE_DATABASE", "telegraf")
        clean_environment.setenv("INFLUXDB3_IMPORT_INFLUXDB_VERSION", "1")

        # the five older names still configure the plugin on their own
        assert load().source_url == "http://old"

        clean_environment.setenv("INFLUXDB3_IMPORT_SOURCE_URL", "http://new")
        assert load().source_url == "http://new"

    def test_file_and_body_override_in_order(self, clean_environment, tmp_path):
        clean_environment.setenv("INFLUXDB3_IMPORT_DEST_DATABASE", "from_env")
        (tmp_path / "cfg.toml").write_text('dest_database = "from_toml"\n')

        assert load(REQUIRED_ARGS).dest_database == "from_env"

        args = {**REQUIRED_ARGS, "dest_database": "from_args"}
        assert load(args).dest_database == "from_args"

        args = {**args, "config_file_path": "cfg.toml"}
        assert load(args).dest_database == "from_toml"
        assert load(args, {"dest_database": "from_body"}).dest_database == "from_body"

    def test_config_file_path_comes_from_the_environment(
        self, clean_environment, tmp_path
    ):
        (tmp_path / "from_env.toml").write_text('dest_database = "env_toml"\n')
        (tmp_path / "from_args.toml").write_text('dest_database = "args_toml"\n')
        clean_environment.setenv("INFLUXDB3_IMPORT_CONFIG_FILE_PATH", "from_env.toml")

        assert load(REQUIRED_ARGS).dest_database == "env_toml"

        args = {**REQUIRED_ARGS, "config_file_path": "from_args.toml"}
        assert load(args).dest_database == "args_toml"

    def test_headers_and_the_query_string_sit_above_the_body(self, clean_environment):
        body = {**REQUIRED_ARGS, "target_batch_size": "11"}
        assert load(body=body).target_batch_size == 11

        headers = {"X-Influxdb3-Import-Target-Batch-Size": "22"}
        assert load(body=body, headers=headers).target_batch_size == 22

        query = {"target_batch_size": "33"}
        assert load(body=body, headers=headers, query=query).target_batch_size == 33

    def test_a_setting_header_is_matched_whatever_its_casing(self, clean_environment):
        headers = {"x-influxdb3-import-dest-database": "from-header"}
        assert load(REQUIRED_ARGS, headers=headers).dest_database == "from-header"

    def test_a_credential_under_the_setting_prefix_is_not_a_setting(
        self, clean_environment
    ):
        # ImportConfig has no source_token field, so reading that header as a
        # setting would have raised instead of loading
        headers = {
            "user-agent": "curl",
            "X-Influxdb3-Import-Source-Token": "tok",
            "X-Influxdb3-Import-Dest-Database": "from-header",
        }
        assert load(REQUIRED_ARGS, headers=headers).dest_database == "from-header"

    def test_body_may_not_name_a_config_file(self, clean_environment):
        with pytest.raises(ValueError) as failure:
            load(REQUIRED_ARGS, {"config_file_path": "cfg.toml"})
        assert "Request body may not set 'config_file_path'" in str(failure.value)

    def test_unknown_key_is_named(self, clean_environment, tmp_path):
        with pytest.raises(ValueError) as failure:
            load({**REQUIRED_ARGS, "target_databse": "typo"})
        assert "Trigger arguments may not set 'target_databse'" in str(failure.value)

        (tmp_path / "cfg.toml").write_text('source_token = "leftover"\n')
        with pytest.raises(ValueError) as failure:
            load({**REQUIRED_ARGS, "config_file_path": "cfg.toml"})
        assert "Config file may not set 'source_token'" in str(failure.value)

    def test_values_are_coerced_and_checked(self, clean_environment):
        config = load(
            {
                **REQUIRED_ARGS,
                "dry_run": "false",
                "table_filter": "cpu.mem.disk",
                "query_interval_ms": "250",
            }
        )
        assert config.influxdb_version == 1
        assert config.dry_run is False
        assert config.table_filter == ["cpu", "mem", "disk"]
        assert config.query_interval_ms == 250

        with pytest.raises(ValueError) as failure:
            load({**REQUIRED_ARGS, "import_direction": "oldest"})
        assert "import_direction must be one of" in str(failure.value)

        with pytest.raises(ValueError) as failure:
            load({**REQUIRED_ARGS, "influxdb_version": "4"})
        assert "influxdb_version must be one of" in str(failure.value)

    def test_required_settings_are_named(self, clean_environment):
        with pytest.raises(ValueError) as failure:
            load({"source_database": "telegraf", "influxdb_version": "1"})
        assert "source_url is required" in str(failure.value)

    def test_an_empty_table_filter_means_every_table(self, clean_environment):
        assert load(REQUIRED_ARGS, {"table_filter": []}).table_filter is None
        assert load({**REQUIRED_ARGS, "table_filter": ""}).table_filter is None
        assert load({**REQUIRED_ARGS, "table_filter": "."}).table_filter is None
        assert load({**REQUIRED_ARGS, "table_filter": "cpu.mem"}).table_filter == [
            "cpu",
            "mem",
        ]

    def test_config_file_path_is_trimmed(self, clean_environment, tmp_path):
        (tmp_path / "cfg.toml").write_text('dest_database = "from_toml"\n')
        # a trigger argument arrives exactly as written, spaces included
        args = {**REQUIRED_ARGS, "config_file_path": " cfg.toml "}
        assert load(args).dest_database == "from_toml"


class TestCheckQueryResult:
    """InfluxDB reports a failed statement with HTTP 200 and an error in the body."""

    def test_statement_error_is_raised(self):
        with pytest.raises(SourceQueryError) as failure:
            check_query_result(
                {"results": [{"statement_id": 0, "error": "max-select-point"}]}
            )
        assert "max-select-point" in str(failure.value)

    def test_empty_result_is_not_an_error(self):
        empty = {"results": [{"statement_id": 0}]}
        assert check_query_result(empty) is empty


class TestCountRowsInResult:
    """COUNT(*) answers with one column per field, none of them the row count."""

    def test_largest_per_field_count_wins(self):
        result = {
            "results": [
                {
                    "series": [
                        {
                            "columns": ["time", "count_a_rare", "count_z_dense"],
                            "values": [["1970-01-01T00:00:00Z", 1, 10]],
                        }
                    ]
                }
            ]
        }
        assert count_rows_in_result(result) == 10

    def test_missing_series_counts_nothing(self):
        assert count_rows_in_result({"results": [{"statement_id": 0}]}) == 0


class TestCheckpointPrecision:
    """A pause checkpoint has to survive a round trip without losing nanoseconds."""

    def test_nanoseconds_survive_the_round_trip(self):
        rendered = format_nanoseconds_iso(1_700_000_000_000_000_500)
        assert rendered == "2023-11-14T22:13:20.000000500+00:00"
        assert parse_timestamp_to_nanoseconds(rendered) == 1_700_000_000_000_000_500

    def test_source_spellings_parse_to_the_same_instant(self):
        # v1 trims trailing zeros, v3 pads to nine digits
        assert parse_timestamp_to_nanoseconds(
            "2023-11-14T22:13:20.0000005Z"
        ) == parse_timestamp_to_nanoseconds("2023-11-14T22:13:20.000000500Z")


class TestWriteToDestination:
    def test_lines_are_batched_into_one_write(self):
        local = FakeLocal()
        success, error = write_to_destination(
            local, "clean_db", [FakeBuilder("a f=1i 1"), FakeBuilder("b f=2i 2")], "task"
        )
        assert (success, error) == (True, None)
        assert local.writes == [("clean_db", "a f=1i 1\nb f=2i 2")]

    def test_empty_database_writes_to_the_trigger_database(self):
        local = FakeLocal()
        write_to_destination(local, "", [FakeBuilder("a f=1i 1")], "task")
        assert local.writes == [(None, "a f=1i 1")]

    def test_a_write_that_keeps_failing_is_reported_not_raised(self):
        # no_sync makes the write raise, so the retries in write_data are live
        local = FakeLocal(fail_write="wal is full")
        success, error = write_to_destination(
            local, "clean_db", [FakeBuilder("a f=1i 1")], "task"
        )
        assert (success, error) == (False, "wal is full")


class TestResumeStopsOnPause:
    """A resumed run must stay resumable when the user pauses it again."""

    def test_pause_halts_the_loop_and_leaves_the_import_open(self, monkeypatch):
        visited, completed_state = [], []

        def fake_import_table(local, config, credentials, import_id, measurement, *a, **k):
            visited.append(measurement)
            if measurement == "t1":
                return {"measurement": measurement, "status": "completed",
                        "rows_imported": 10, "errors": []}
            return {"measurement": measurement, "status": "paused",
                    "rows_imported": 3, "errors": [], "paused_at_time": "2026-01-01T00:00:00+00:00"}

        monkeypatch.setattr(import_module, "import_table", fake_import_table)
        monkeypatch.setattr(import_module, "get_source_measurements",
                            lambda *a, **k: ["t1", "t2", "t3"])
        monkeypatch.setattr(import_module, "write_import_state", lambda *a, **k: None)
        monkeypatch.setattr(import_module, "_write_import_pause_state",
                            lambda *a, **k: completed_state.append(k))

        report = import_module.resume_incomplete_import(
            FakeLocal(),
            ImportConfig(source_url="u", source_database="d", influxdb_version=1),
            {},
            "imp",
            [{"table_name": "t2", "status": "paused", "rows_imported": 0,
              "paused_at_time": ""}],
            "task",
        )

        assert visited == ["t1", "t2"]  # t3 is left alone
        assert report["status"] == "paused"
        assert report["paused_on_table"] == "t2"
        assert report["rows_imported"] == 13
        assert completed_state == []  # the import is not marked complete


START = datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)
BASE_NS = 1_700_000_000_000_000_000


def window_series(*offsets_in_seconds):
    """A query answer holding one row per offset."""
    return {
        "results": [
            {
                "series": [
                    {
                        "columns": ["time", "value"],
                        "values": [
                            [(START + timedelta(seconds=offset)).isoformat(), offset]
                            for offset in offsets_in_seconds
                        ],
                    }
                ]
            }
        ]
    }


EMPTY_ANSWER = {"results": [{"statement_id": 0}]}


def run_import_table(monkeypatch, answers, pause_after_windows, direction="oldest_first"):
    """Walk import_table over scripted window answers, pausing part way."""
    state = {}
    checks = {"count": 0}
    replies = iter(answers)

    def pause_state(*args, **kwargs):
        checks["count"] += 1
        return (
            import_module.ImportPauseState.PAUSED
            if checks["count"] > pause_after_windows
            else import_module.ImportPauseState.RUNNING
        )

    def record_state(local, import_id, table, status, rows, task_id,
                     paused_at_time=None, no_sync=False, errors=None,
                     failed_windows=None, error_limit=None):
        state.update(
            status=status, paused_at_time=paused_at_time, rows=rows, errors=errors
        )

    monkeypatch.setattr(import_module, "get_import_pause_state", pause_state)
    monkeypatch.setattr(import_module, "write_import_state", record_state)
    monkeypatch.setattr(import_module, "write_to_destination", lambda *a: (True, None))
    monkeypatch.setattr(
        import_module,
        "prepare_table_import",
        lambda *a, **k: (START, START + timedelta(seconds=20), 8, {}, [], []),
    )
    monkeypatch.setattr(
        import_module, "query_source_influxdb", lambda *a, **k: next(replies)
    )
    monkeypatch.setattr(
        import_module,
        "convert_influxql_to_line_protocol",
        lambda local, measurement, series, *a, **k: [
            FakeBuilder("line") for _ in series["values"]
        ],
    )

    config = ImportConfig(
        source_url="http://src",
        source_database="telegraf",
        influxdb_version=1,
        import_direction=direction,
        query_interval_ms=0,
    )
    result = import_module.import_table(
        FakeLocal(), config, {}, "imp", "cpu", None, None, "task"
    )
    return result, state


class TestPauseCheckpoint:
    """The checkpoint has to name a row that was written, not a window edge."""

    def test_an_empty_window_does_not_move_the_checkpoint(self, monkeypatch):
        # first window writes rows up to +7s, the second one is empty
        result, state = run_import_table(
            monkeypatch, [window_series(0, 7), EMPTY_ANSWER], pause_after_windows=2
        )

        assert result["status"] == "paused"
        assert state["paused_at_time"] == format_nanoseconds_iso(BASE_NS + 7 * 10**9)
        # the row sitting exactly on the next window edge is still ahead of it
        assert parse_timestamp_to_nanoseconds(state["paused_at_time"]) < BASE_NS + 8 * 10**9

    def test_the_response_repeats_the_stored_checkpoint(self, monkeypatch):
        result, state = run_import_table(
            monkeypatch, [window_series(0, 7), EMPTY_ANSWER], pause_after_windows=2
        )
        assert result["paused_at_time"] == state["paused_at_time"]

    def test_nothing_written_leaves_an_empty_checkpoint(self, monkeypatch):
        # paused before the first window, so no row was ever written
        result, state = run_import_table(monkeypatch, [], pause_after_windows=0)

        assert result["status"] == "paused"
        assert state["paused_at_time"] == ""
        assert result["paused_at_time"] == ""

    def test_newest_first_checkpoints_the_oldest_written_row(self, monkeypatch):
        result, state = run_import_table(
            monkeypatch,
            [window_series(12, 19)],
            pause_after_windows=1,
            direction="newest_first",
        )
        assert state["paused_at_time"] == format_nanoseconds_iso(BASE_NS + 12 * 10**9)


class TestPreparationFailure:
    """A table that fails before its first window still has to be resumable."""

    def test_failure_writes_a_paused_state_and_propagates(self, monkeypatch):
        state = {}

        def record_state(local, import_id, table, status, rows, task_id,
                         paused_at_time=None, no_sync=False, errors=None,
                         failed_windows=None, error_limit=None):
            state.update(status=status, rows=rows)

        def refuse(*args, **kwargs):
            raise SourceQueryError("Source query failed: max-select-point limit exceeded")

        monkeypatch.setattr(import_module, "prepare_table_import", refuse)
        monkeypatch.setattr(import_module, "write_import_state", record_state)

        config = ImportConfig(
            source_url="http://src", source_database="telegraf", influxdb_version=1
        )
        with pytest.raises(SourceQueryError):
            import_module.import_table(
                FakeLocal(), config, {}, "imp", "cpu", None, None, "task"
            )

        # 'pending' would leave the table invisible to a resume
        assert state == {"status": "paused", "rows": 0}


class TestTimestampZones:
    """InfluxQL refuses a bound without an offset, so parsing must supply one."""

    @pytest.mark.parametrize(
        "written",
        ["2026-01-01", "2026-01-01T00:00:00", "2026-01-01 00:00:00",
         "2026-01-01T00:00:00Z", "1767225600"],
    )
    def test_every_accepted_spelling_lands_on_the_same_instant(self, written):
        parsed = import_module.parse_timestamp(written)
        assert parsed.tzinfo is not None
        assert parsed == datetime(2026, 1, 1, tzinfo=timezone.utc)


class TestQuoteIdentifier:
    """InfluxQL escapes inside an identifier with a backslash, not by doubling."""

    @pytest.mark.parametrize(
        "name,quoted",
        [
            ("sensors", '"sensors"'),
            ('wei"rd', '"wei\\"rd"'),
            ("back\\slash", '"back\\\\slash"'),
            ("single'quote", '"single\'quote"'),
        ],
    )
    def test_names_are_quoted_for_influxql(self, name, quoted):
        assert import_module.quote_influxql_identifier(name) == quoted

    def test_a_quoted_name_reaches_the_query(self, monkeypatch):
        seen = []
        monkeypatch.setattr(
            import_module,
            "query_source_influxdb",
            lambda local, config, creds, query, task: seen.append(query) or {"results": [{}]},
        )
        config = ImportConfig(
            source_url="http://src", source_database="telegraf", influxdb_version=1
        )
        import_module.get_field_keys(FakeLocal(), config, {}, 'wei"rd', "task")
        assert seen == ['SHOW FIELD KEYS FROM "wei\\"rd"']


class TestRowCountAcrossResume:
    """import_state has to hold the table's total, not the latest attempt's."""

    def test_a_resumed_table_continues_the_count(self, monkeypatch):
        state = {}

        def record_state(local, import_id, table, status, rows, task_id,
                         paused_at_time=None, no_sync=False, errors=None,
                         failed_windows=None, error_limit=None):
            state.update(status=status, rows=rows)

        monkeypatch.setattr(import_module, "write_import_state", record_state)
        monkeypatch.setattr(import_module, "get_import_pause_state",
                            lambda *a, **k: import_module.ImportPauseState.RUNNING)
        monkeypatch.setattr(import_module, "write_to_destination", lambda *a: (True, None))
        monkeypatch.setattr(
            import_module,
            "prepare_table_import",
            lambda *a, **k: (START, START + timedelta(seconds=20), 30, {}, [], []),
        )
        monkeypatch.setattr(
            import_module, "query_source_influxdb", lambda *a, **k: window_series(0, 7)
        )
        monkeypatch.setattr(
            import_module,
            "convert_influxql_to_line_protocol",
            lambda local, measurement, series, *a, **k: [
                FakeBuilder("line") for _ in series["values"]
            ],
        )

        config = ImportConfig(
            source_url="http://src", source_database="telegraf", influxdb_version=1,
            query_interval_ms=0,
        )
        result = import_module.import_table(
            FakeLocal(), config, {}, "imp", "cpu", None, None, "task",
            rows_already_imported=5,
        )

        # two rows in the window on top of the five an earlier attempt wrote
        assert result["rows_imported"] == 7
        assert state == {"status": "completed", "rows": 7}


class TestErrorsColumn:
    """import_state carries the windows a table failed to write."""

    def test_nothing_failed_still_records_the_column(self):
        assert json.loads(import_module.errors_as_json()) == {
            "failed_windows": 0,
            "errors": [],
        }
        assert json.loads(import_module.errors_as_json([])) == {
            "failed_windows": 0,
            "errors": [],
        }

    def test_failures_are_kept_with_their_reason(self):
        failures = [
            {"time_range": "a to b", "error": "invalid column type for column 'room'"}
        ]
        stored = json.loads(import_module.errors_as_json(failures))
        assert stored == {"failed_windows": 1, "errors": failures}

    def test_a_long_list_keeps_its_scale_and_its_first_entries(self):
        failures = [{"time_range": f"w{i}", "error": "same reason"} for i in range(240)]
        stored = json.loads(import_module.errors_as_json(failures))

        # the count is the truth, the list is a sample small enough for one point
        assert stored["failed_windows"] == 240
        assert len(stored["errors"]) == import_module.STORED_ERRORS
        assert stored["errors"] == failures[: import_module.STORED_ERRORS]

    def test_the_state_row_carries_what_the_table_failed(self, monkeypatch):
        written = {}

        class RecordingBuilder(FakeBuilder):
            def __init__(self, measurement):
                super().__init__(measurement)

            def tag(self, key, value):
                return self

            def string_field(self, key, value):
                written[key] = value
                return self

            def int64_field(self, key, value):
                written[key] = value
                return self

            def time_ns(self, value):
                return self

        monkeypatch.setattr(builtins, "LineBuilder", RecordingBuilder)
        failures = [{"time_range": "a to b", "error": "write refused"}]
        import_module.write_import_state(
            FakeLocal(), "imp", "cpu", "completed", 7, "task", errors=failures
        )

        assert written["status"] == "completed"
        assert json.loads(written["errors"]) == {
            "failed_windows": 1,
            "errors": failures,
        }


def walk_table(monkeypatch, answers, pause_after=None, direction="oldest_first", **carry):
    """import_table over scripted window answers, recording state rows and queries."""
    written, queries = [], []
    checks = {"count": 0}
    replies = iter(answers)

    def pause_state(*args, **kwargs):
        checks["count"] += 1
        if pause_after is not None and checks["count"] > pause_after:
            return import_module.ImportPauseState.PAUSED
        return import_module.ImportPauseState.RUNNING

    def record_state(local, import_id, table, status, rows, task_id,
                     paused_at_time=None, no_sync=False, errors=None,
                     failed_windows=None, error_limit=None):
        written.append({
            "status": status,
            "rows": rows,
            "checkpoint": paused_at_time,
            "errors": errors,
            "failed_windows": failed_windows,
            "error_limit": error_limit,
        })

    def query(local, config, credentials, sql, task_id):
        queries.append(sql)
        return next(replies)

    monkeypatch.setattr(import_module, "get_import_pause_state", pause_state)
    monkeypatch.setattr(import_module, "write_import_state", record_state)
    monkeypatch.setattr(import_module, "write_to_destination", lambda *a: (True, None))
    monkeypatch.setattr(
        import_module,
        "prepare_table_import",
        lambda *a, **k: (START, START + timedelta(seconds=20), 8, {}, [], []),
    )
    monkeypatch.setattr(import_module, "query_source_influxdb", query)
    monkeypatch.setattr(
        import_module,
        "convert_influxql_to_line_protocol",
        lambda local, measurement, series, *a, **k: [
            FakeBuilder("line") for _ in series["values"]
        ],
    )

    config = ImportConfig(
        source_url="http://src",
        source_database="telegraf",
        influxdb_version=1,
        import_direction=direction,
        query_interval_ms=0,
    )
    result = import_module.import_table(
        FakeLocal(), config, {}, "imp", "cpu", None, None, "task", **carry
    )
    return result, written, queries


class TestEscapeStringLiteral:
    """import_id and measurement names reach SQL as literals, not identifiers."""

    @pytest.mark.parametrize(
        "value,escaped",
        [
            ("plain", "plain"),
            ("x'quote", "x''quote"),
            ("x' OR '1'='1", "x'' OR ''1''=''1"),
        ],
    )
    def test_a_quote_is_doubled(self, value, escaped):
        assert import_module.escape_string_literal(value) == escaped


class TestCarriedAcrossResume:
    """A resumed table keeps the position and the failures of earlier attempts."""

    def test_a_stop_before_the_first_window_keeps_the_position(self, monkeypatch):
        checkpoint_ns = BASE_NS + 5_000_000_000
        result, written, _ = walk_table(
            monkeypatch, [], pause_after=0,
            rows_already_imported=41, imported_up_to_ns=checkpoint_ns,
        )

        # without the carry this table would report an empty checkpoint and
        # start over on the next resume
        assert result["status"] == "paused"
        assert result["rows_imported"] == 41
        assert written[-1]["checkpoint"] == import_module.format_nanoseconds_iso(
            checkpoint_ns
        )

    def test_earlier_failures_are_added_to_rather_than_replaced(self, monkeypatch):
        earlier = [{"time_range": "w0", "error": "refused"}]
        result, written, _ = walk_table(
            monkeypatch, [], pause_after=0,
            windows_already_failed=240, errors_already_recorded=earlier,
        )

        # the count is the total, while the sample is only what fitted
        assert written[-1]["failed_windows"] == 240
        assert written[-1]["errors"] == earlier
        assert result["failed_windows"] == 240

    def test_preparation_failing_does_not_erase_the_progress(self, monkeypatch):
        written = []
        checkpoint_ns = BASE_NS + 5_000_000_000

        def record_state(local, import_id, table, status, rows, task_id,
                         paused_at_time=None, no_sync=False, errors=None,
                         failed_windows=None, error_limit=None):
            written.append((status, rows, paused_at_time))

        def refuse(*args, **kwargs):
            raise import_module.SourceQueryError("Source query failed: limit exceeded")

        monkeypatch.setattr(import_module, "prepare_table_import", refuse)
        monkeypatch.setattr(import_module, "write_import_state", record_state)

        config = ImportConfig(
            source_url="http://src", source_database="telegraf", influxdb_version=1
        )
        with pytest.raises(import_module.SourceQueryError):
            import_module.import_table(
                FakeLocal(), config, {}, "imp", "cpu", None, None, "task",
                rows_already_imported=41, imported_up_to_ns=checkpoint_ns,
            )

        assert written == [
            ("paused", 41, import_module.format_nanoseconds_iso(checkpoint_ns))
        ]

    def test_an_empty_range_does_not_reset_a_resumed_table(self, monkeypatch):
        written = []

        def record_state(local, import_id, table, status, rows, task_id,
                         paused_at_time=None, no_sync=False, errors=None,
                         failed_windows=None, error_limit=None):
            written.append((status, rows, failed_windows))

        monkeypatch.setattr(import_module, "write_import_state", record_state)
        monkeypatch.setattr(
            import_module, "prepare_table_import", lambda *a, **k: (None, None, 0, {}, [], [])
        )

        config = ImportConfig(
            source_url="http://src", source_database="telegraf", influxdb_version=1
        )
        result = import_module.import_table(
            FakeLocal(), config, {}, "imp", "cpu", None, None, "task",
            rows_already_imported=10, windows_already_failed=3,
        )

        assert result["rows_imported"] == 10
        assert written == [("completed", 10, 3)]


class TestProgressRows:
    """A row written mid-import has to be resumable and small."""

    def test_it_carries_the_checkpoint_of_the_window_just_written(self, monkeypatch):
        _, written, _ = walk_table(monkeypatch, [window_series(0, 1, 2)], pause_after=1)

        progress = [row for row in written if row["status"] == "in_progress"]
        assert len(progress) == 1
        newest = import_module.parse_timestamp_to_nanoseconds(
            (START + timedelta(seconds=2)).isoformat()
        )
        assert progress[0]["checkpoint"] == import_module.format_nanoseconds_iso(newest)

    def test_it_samples_fewer_reasons_than_a_terminal_row(self, monkeypatch):
        _, written, _ = walk_table(monkeypatch, [window_series(0)], pause_after=1)

        progress = [row for row in written if row["status"] == "in_progress"]
        assert progress[0]["error_limit"] == import_module.PROGRESS_ERRORS
        assert written[-1]["error_limit"] is None  # the default, STORED_ERRORS


class TestFirstWindowClamp:
    """A datetime bound cannot hold the nanoseconds of a checkpoint."""

    def test_oldest_first_states_the_checkpoint_on_the_lower_bound(self, monkeypatch):
        checkpoint_ns = BASE_NS + 123_456_789
        _, _, queries = walk_table(
            monkeypatch, [window_series(0)], pause_after=1,
            imported_up_to_ns=checkpoint_ns,
        )
        bound = import_module.format_nanoseconds_iso(checkpoint_ns + 1)
        assert f"time >= '{bound}'" in queries[0]

    def test_newest_first_states_it_on_the_upper_bound(self, monkeypatch):
        checkpoint_ns = BASE_NS + 123_456_789
        _, _, queries = walk_table(
            monkeypatch, [window_series(0)], pause_after=1,
            direction="newest_first", imported_up_to_ns=checkpoint_ns,
        )
        # the upper bound is exclusive, so naming the checkpoint excludes its row
        bound = import_module.format_nanoseconds_iso(checkpoint_ns)
        assert f"time < '{bound}'" in queries[0]

    def test_later_windows_are_left_alone(self, monkeypatch):
        checkpoint_ns = BASE_NS + 123_456_789
        _, _, queries = walk_table(
            monkeypatch, [window_series(0), window_series(9)], pause_after=2,
            imported_up_to_ns=checkpoint_ns,
        )
        bound = import_module.format_nanoseconds_iso(checkpoint_ns + 1)
        assert bound in queries[0]
        assert bound not in queries[1]

    def test_a_fresh_import_is_not_clamped(self, monkeypatch):
        _, _, queries = walk_table(monkeypatch, [window_series(0)], pause_after=1)
        assert ".000000000+00:00'" not in queries[0].split("AND")[0]


class TestEmptyResultsEnvelope:
    """A source may answer with no statements at all."""

    def test_it_reads_as_no_data_rather_than_raising(self, monkeypatch):
        result, _, _ = walk_table(monkeypatch, [{"results": []}] * 3)
        assert result["status"] == "completed"
        assert result["rows_imported"] == 0


class TestResumeCountsSkippedTables:
    """A table that finished before the pause is skipped, not forgotten."""

    def test_its_rows_and_failures_reach_the_report(self, monkeypatch):
        class LocalWithState(FakeLocal):
            def query(self, sql):
                # the already-completed check for the table this run skips
                return [
                    {
                        "status": "completed",
                        "rows_imported": 60,
                        "errors": json.dumps({"failed_windows": 7, "errors": []}),
                    }
                ]

        monkeypatch.setattr(
            import_module, "get_source_measurements", lambda *a, **k: ["done", "left"]
        )
        monkeypatch.setattr(
            import_module, "_write_import_pause_state", lambda *a, **k: None
        )
        monkeypatch.setattr(
            import_module,
            "import_table",
            lambda *a, **k: {
                "measurement": "left",
                "status": "completed",
                "rows_imported": 240,
                "errors": [],
                "failed_windows": 0,
            },
        )

        config = ImportConfig(
            source_url="http://src", source_database="telegraf", influxdb_version=1
        )
        report = import_module.resume_incomplete_import(
            LocalWithState(),
            config,
            {},
            "imp",
            [
                {
                    "table_name": "left",
                    "status": "paused",
                    "rows_imported": 0,
                    "paused_at_time": "",
                    "errors": {},
                }
            ],
            "task",
        )

        assert report["tables"] == {"total": 2, "completed": 2}
        assert report["rows_imported"] == 300  # 60 skipped plus 240 imported
        assert report["errors"] == 7  # the skipped table's failures count too
