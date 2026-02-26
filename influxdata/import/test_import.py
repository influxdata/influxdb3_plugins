"""Tests for import.py functions."""

import pytest
from unittest.mock import Mock, patch

# Note: The module is named "import" which is a Python keyword
# We need to use importlib to import it
import importlib
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

    def test_with_token_returns_bearer_auth(self):
        result = _build_v3_headers("my-token")
        assert result == {
            "Content-Type": "application/json",
            "Authorization": "Bearer my-token",
        }

    def test_without_token_returns_content_type_only(self):
        result = _build_v3_headers(None)
        assert result == {"Content-Type": "application/json"}

    def test_empty_token_returns_content_type_only(self):
        result = _build_v3_headers("")
        assert result == {"Content-Type": "application/json"}


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
        assert result == {"error": "Unsupported influxdb_version: 0. Must be 1, 2, or 3."}

    def test_version_4_is_invalid(self):
        result = _validate_source_params({
            "source_url": "http://localhost:8086",
            "influxdb_version": 4,
        })
        assert result == {"error": "Unsupported influxdb_version: 4. Must be 1, 2, or 3."}

    def test_string_version_is_invalid(self):
        result = _validate_source_params({
            "source_url": "http://localhost:8086",
            "influxdb_version": "3",
        })
        assert result == {"error": "Unsupported influxdb_version: 3. Must be 1, 2, or 3."}


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
                "source_token": "my-token",
            },
            session=mock_session,
        )

        assert result == {"databases": ["import", "test"]}
        mock_session.get.assert_called_once()
        call_args = mock_session.get.call_args
        assert "/api/v3/configure/database" in call_args[0][0]
        assert call_args[1]["params"] == {"format": "json"}


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
                "source_token": "my-token",
            },
            session=mock_session,
        )

        assert result == {"tables": ["import_pause_state"]}
        mock_session.get.assert_called_once()
        call_args = mock_session.get.call_args
        assert "/api/v3/query_sql" in call_args[0][0]
        assert call_args[1]["params"] == {"db": "mydb", "q": "SHOW TABLES", "format": "json"}


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

        config = ImportConfig(
            source_url="http://localhost",
            source_database="mydb",
            influxdb_version=3,
            source_token="my-v3-token",
        )

        query_source_influxdb(mock_influxdb3_local, config, "SHOW MEASUREMENTS", "test-task")

        # Verify the Authorization header uses Bearer format
        call_kwargs = mock_session.get.call_args
        headers = call_kwargs.kwargs.get("headers", call_kwargs[1].get("headers", {}))
        assert headers.get("Authorization") == "Bearer my-v3-token"

    def test_v3_without_token_raises_error(self):
        """Verify v3 without token raises ValueError."""
        mock_influxdb3_local = Mock()

        config = ImportConfig(
            source_url="http://localhost",
            source_database="mydb",
            influxdb_version=3,
            source_token=None,
        )

        with pytest.raises(ValueError, match="InfluxDB v3 requires source_token"):
            query_source_influxdb(mock_influxdb3_local, config, "SHOW MEASUREMENTS", "test-task")
