"""Tests for pure (non-async, non-network) methods of RestDAO."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from ami.core.storage_types import StorageType
from ami.implementations.rest.rest_dao import RestDAO
from ami.models.storage_config import StorageConfig

_BASIC_HEADER_COUNT = 2
_ANSWER_INT = 42


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _make_config(**overrides: Any) -> StorageConfig:
    """Build a StorageConfig with REST defaults."""
    defaults: dict[str, Any] = {
        "storage_type": StorageType.REST,
        "host": "api.example.com",
        "port": 8080,
    }
    defaults.update(overrides)
    return StorageConfig(**defaults)


def _make_dao(
    config: StorageConfig | None = None,
    collection: str = "users",
) -> RestDAO:
    """Create a RestDAO with a mocked model class."""
    dao = RestDAO(model_cls=MagicMock(), config=config)
    dao.collection_name = collection
    return dao


# ==================================================================
# _build_base_url  (static method)
# ==================================================================


class TestBuildBaseUrl:
    """Tests for RestDAO._build_base_url static method."""

    def test_connection_string_used_directly(self) -> None:
        cfg = _make_config(connection_string="https://custom.api.io/v2")
        result = RestDAO._build_base_url(cfg)
        assert result == "https://custom.api.io/v2"

    def test_connection_string_trailing_slash_stripped(self) -> None:
        cfg = _make_config(connection_string="https://custom.api.io/v2/")
        result = RestDAO._build_base_url(cfg)
        assert result == "https://custom.api.io/v2"

    def test_port_443_uses_https(self) -> None:
        cfg = _make_config(host="secure.io", port=443)
        result = RestDAO._build_base_url(cfg)
        assert result.startswith("https://")
        assert "secure.io:443" in result

    def test_non_443_port_uses_http(self) -> None:
        cfg = _make_config(host="local.dev", port=8080)
        result = RestDAO._build_base_url(cfg)
        assert result.startswith("http://")
        assert "local.dev:8080" in result

    def test_database_appended_to_url(self) -> None:
        cfg = _make_config(
            host="db.host",
            port=9090,
            database="mydb",
        )
        result = RestDAO._build_base_url(cfg)
        assert result == "http://db.host:9090/mydb"

    def test_no_database_omits_suffix(self) -> None:
        cfg = _make_config(host="db.host", port=9090)
        result = RestDAO._build_base_url(cfg)
        assert result == "http://db.host:9090"

    def test_default_host_is_localhost(self) -> None:
        cfg = _make_config(host=None, port=8080)
        result = RestDAO._build_base_url(cfg)
        assert "localhost:8080" in result

    def test_default_port_is_443(self) -> None:
        """When port is None, _build_base_url defaults to 443."""
        cfg = _make_config(host="h.io", port=None)
        # StorageConfig sets REST default port to 443 via
        # model_post_init, so port will already be 443.
        result = RestDAO._build_base_url(cfg)
        assert ":443" in result
        assert result.startswith("https://")

    def test_connection_string_takes_priority(self) -> None:
        cfg = _make_config(
            host="ignored.host",
            port=9999,
            connection_string="https://priority.api/v1",
        )
        result = RestDAO._build_base_url(cfg)
        assert result == "https://priority.api/v1"
        assert "ignored" not in result


# ==================================================================
# _build_url  (instance method)
# ==================================================================


class TestBuildUrl:
    """Tests for RestDAO._build_url instance method."""

    def test_base_url_only(self) -> None:
        dao = _make_dao(config=_make_config(), collection="users")
        url = dao._build_url()
        assert url == f"{dao.base_url}/users"

    def test_with_path(self) -> None:
        dao = _make_dao(config=_make_config(), collection="users")
        url = dao._build_url(path="search")
        assert url == f"{dao.base_url}/users/search"

    def test_with_item_id(self) -> None:
        dao = _make_dao(config=_make_config(), collection="users")
        url = dao._build_url(item_id="42")
        assert url == f"{dao.base_url}/users/42"

    def test_with_path_and_item_id(self) -> None:
        dao = _make_dao(config=_make_config(), collection="orders")
        url = dao._build_url(path="archive", item_id="99")
        assert url == f"{dao.base_url}/orders/archive/99"

    def test_empty_path_ignored(self) -> None:
        dao = _make_dao(config=_make_config(), collection="items")
        url = dao._build_url(path="")
        assert url == f"{dao.base_url}/items"

    def test_none_item_id_ignored(self) -> None:
        dao = _make_dao(config=_make_config(), collection="items")
        url = dao._build_url(item_id=None)
        assert url == f"{dao.base_url}/items"


# ==================================================================
# _prepare_headers  (instance method)
# ==================================================================


class TestPrepareHeaders:
    """Tests for RestDAO._prepare_headers instance method."""

    def test_default_headers_present(self) -> None:
        dao = _make_dao(config=_make_config())
        headers = dao._prepare_headers()
        assert headers["Content-Type"] == "application/json"
        assert headers["Accept"] == "application/json"

    def test_no_config_returns_basic_headers(self) -> None:
        dao = _make_dao(config=None)
        headers = dao._prepare_headers()
        assert headers == {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def test_empty_options_returns_basic_headers(self) -> None:
        cfg = _make_config(options={})
        dao = _make_dao(config=cfg)
        headers = dao._prepare_headers()
        assert len(headers) == _BASIC_HEADER_COUNT

    def test_auth_token_adds_bearer(self) -> None:
        cfg = _make_config(options={"auth_token": "tok-abc123"})
        dao = _make_dao(config=cfg)
        headers = dao._prepare_headers()
        assert headers["Authorization"] == "Bearer tok-abc123"

    def test_api_key_adds_header(self) -> None:
        cfg = _make_config(options={"api_key": "key-xyz789"})
        dao = _make_dao(config=cfg)
        headers = dao._prepare_headers()
        assert headers["X-API-Key"] == "key-xyz789"

    def test_custom_headers_merged(self) -> None:
        cfg = _make_config(
            options={
                "headers": {
                    "X-Custom": "val",
                    "X-Tenant": "acme",
                }
            }
        )
        dao = _make_dao(config=cfg)
        headers = dao._prepare_headers()
        assert headers["X-Custom"] == "val"
        assert headers["X-Tenant"] == "acme"
        # Original headers still present
        assert headers["Content-Type"] == "application/json"

    def test_custom_headers_override_defaults(self) -> None:
        cfg = _make_config(
            options={
                "headers": {
                    "Content-Type": "text/xml",
                }
            }
        )
        dao = _make_dao(config=cfg)
        headers = dao._prepare_headers()
        assert headers["Content-Type"] == "text/xml"

    def test_auth_and_api_key_combined(self) -> None:
        cfg = _make_config(
            options={
                "auth_token": "tok-1",
                "api_key": "key-2",
            }
        )
        dao = _make_dao(config=cfg)
        headers = dao._prepare_headers()
        assert headers["Authorization"] == "Bearer tok-1"
        assert headers["X-API-Key"] == "key-2"


# ==================================================================
# _map_fields  (instance method)
# ==================================================================


class TestMapFields:
    """Tests for RestDAO._map_fields instance method."""

    def test_with_field_mapping_applies_reverse(self) -> None:
        cfg = _make_config(
            options={
                "field_mapping": {
                    "name": "full_name",
                    "email": "email_addr",
                }
            }
        )
        dao = _make_dao(config=cfg)
        data = {
            "full_name": "Alice",
            "email_addr": "a@b.com",
        }
        result = dao._map_fields(data)
        assert result == {
            "name": "Alice",
            "email": "a@b.com",
        }

    def test_unmapped_keys_passed_through(self) -> None:
        cfg = _make_config(options={"field_mapping": {"name": "full_name"}})
        dao = _make_dao(config=cfg)
        data = {"full_name": "Alice", "age": 30}
        result = dao._map_fields(data)
        assert result == {"name": "Alice", "age": 30}

    def test_no_mapping_passthrough(self) -> None:
        cfg = _make_config(options={})
        dao = _make_dao(config=cfg)
        data = {"foo": "bar", "baz": 42}
        result = dao._map_fields(data)
        assert result == data

    def test_none_config_passthrough(self) -> None:
        dao = _make_dao(config=None)
        data = {"x": 1, "y": 2}
        result = dao._map_fields(data)
        assert result == data

    def test_empty_field_mapping_passthrough(self) -> None:
        cfg = _make_config(options={"field_mapping": {}})
        dao = _make_dao(config=cfg)
        data = {"a": "b"}
        result = dao._map_fields(data)
        assert result == {"a": "b"}

    def test_non_dict_field_mapping_passthrough(self) -> None:
        cfg = _make_config(options={"field_mapping": "invalid"})
        dao = _make_dao(config=cfg)
        data = {"a": "b"}
        result = dao._map_fields(data)
        assert result == {"a": "b"}


# ==================================================================
# _extract_data  (instance method)
# ==================================================================


class TestExtractData:
    """Tests for RestDAO._extract_data instance method."""

    def test_configured_response_data_key(self) -> None:
        cfg = _make_config(options={"response_data_key": "payload"})
        dao = _make_dao(config=cfg)
        resp = {
            "payload": [{"id": 1}],
            "meta": {"page": 1},
        }
        assert dao._extract_data(resp) == [{"id": 1}]

    def test_configured_key_missing_probes_common(
        self,
    ) -> None:
        cfg = _make_config(options={"response_data_key": "payload"})
        dao = _make_dao(config=cfg)
        resp = {"data": [{"id": 2}]}
        assert dao._extract_data(resp) == [{"id": 2}]

    def test_probes_data_key(self) -> None:
        dao = _make_dao(config=_make_config())
        resp = {"data": [1, 2, 3], "total": 3}
        assert dao._extract_data(resp) == [1, 2, 3]

    def test_probes_results_key(self) -> None:
        dao = _make_dao(config=_make_config())
        resp = {"results": ["a", "b"]}
        assert dao._extract_data(resp) == ["a", "b"]

    def test_probes_items_key(self) -> None:
        dao = _make_dao(config=_make_config())
        resp = {"items": [10, 20]}
        assert dao._extract_data(resp) == [10, 20]

    def test_probes_records_key(self) -> None:
        dao = _make_dao(config=_make_config())
        resp = {"records": [{"r": 1}]}
        assert dao._extract_data(resp) == [{"r": 1}]

    def test_probe_priority_order(self) -> None:
        """'data' is probed before 'results'."""
        dao = _make_dao(config=_make_config())
        resp = {
            "data": "first",
            "results": "second",
        }
        assert dao._extract_data(resp) == "first"

    def test_raw_dict_passthrough_no_envelope(self) -> None:
        dao = _make_dao(config=_make_config())
        resp = {"id": 1, "name": "Alice"}
        assert dao._extract_data(resp) == resp

    def test_non_dict_passthrough_list(self) -> None:
        dao = _make_dao(config=_make_config())
        resp = [{"id": 1}, {"id": 2}]
        assert dao._extract_data(resp) == resp

    def test_non_dict_passthrough_string(self) -> None:
        dao = _make_dao(config=_make_config())
        assert dao._extract_data("raw-text") == "raw-text"

    def test_non_dict_passthrough_int(self) -> None:
        dao = _make_dao(config=_make_config())
        assert dao._extract_data(_ANSWER_INT) == _ANSWER_INT

    def test_none_config_probes_common_keys(self) -> None:
        dao = _make_dao(config=None)
        resp = {"items": ["x"]}
        assert dao._extract_data(resp) == ["x"]
