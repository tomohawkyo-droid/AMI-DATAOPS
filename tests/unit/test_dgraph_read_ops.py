"""Tests for dgraph_read module-level async functions.

Each function is tested with mocked pydgraph transactions
to verify query construction, result parsing, and error paths.
"""

from __future__ import annotations

import json
import sys
import types
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel as PydanticBaseModel

# Ensure pydgraph mock is present when running in isolation.
if "pydgraph" not in sys.modules:
    _pdg = types.ModuleType("pydgraph")
    _pdg.DgraphClient = MagicMock
    _pdg.DgraphClientStub = MagicMock
    _pdg.Mutation = MagicMock
    _pdg.Operation = MagicMock
    sys.modules["pydgraph"] = _pdg

from ami.core.exceptions import StorageError
from ami.implementations.graph.dgraph_read import (
    count,
    exists,
    find,
    find_by_id,
    find_one,
    get_model_fields,
    get_model_indexes,
    get_model_info,
    get_model_schema,
    list_databases,
    list_models,
    list_schemas,
    raw_read_query,
)
from ami.implementations.graph.dgraph_read import (
    test_connection as _check_conn,
)

_COLL = "test_nodes"
_COUNT_42 = 42
_ZERO = 0
_ONE = 1
_TWO = 2
_QWT = "ami.implementations.graph.dgraph_read.query_with_timeout"


class _SimpleModel(PydanticBaseModel):
    uid: str | None = None
    name: str = ""


def _dao(*, connected: bool = True) -> MagicMock:
    """Build a mock DAO with the fields dgraph_read expects."""
    d = MagicMock()
    d.collection_name = _COLL
    d.model_cls = _SimpleModel
    d.client = MagicMock() if connected else None
    return d


def _resp(data: Any) -> MagicMock:
    """Build a mock Dgraph query response."""
    r = MagicMock()
    r.json = json.dumps(data)
    return r


class TestFindById:
    """Verify find_by_id for UID and app-uid lookups."""

    @pytest.mark.asyncio
    async def test_uid_query_returns_model(self) -> None:
        node = {
            "uid": "0xabc",
            "dgraph.type": [_COLL],
            f"{_COLL}.name": "alpha",
            f"{_COLL}.app_uid": "app-1",
        }
        resp = _resp({"node": [node]})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            result = await find_by_id(_dao(), "0xabc")
        assert result is not None
        assert result.name == "alpha"

    @pytest.mark.asyncio
    async def test_uid_wrong_type_returns_none(self) -> None:
        node = {
            "uid": "0xabc",
            "dgraph.type": ["OtherType"],
            f"{_COLL}.name": "alpha",
        }
        resp = _resp({"node": [node]})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            assert await find_by_id(_dao(), "0xabc") is None

    @pytest.mark.asyncio
    async def test_app_uid_returns_model(self) -> None:
        node = {
            "uid": "0x1",
            "dgraph.type": [_COLL],
            f"{_COLL}.name": "beta",
            f"{_COLL}.app_uid": "my-id",
        }
        resp = _resp({"node": [node]})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            result = await find_by_id(_dao(), "my-id")
        assert result is not None
        assert result.name == "beta"

    @pytest.mark.asyncio
    async def test_empty_node_returns_none(self) -> None:
        resp = _resp({"node": []})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            assert await find_by_id(_dao(), "0x999") is None

    @pytest.mark.asyncio
    async def test_not_connected_raises(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await find_by_id(_dao(connected=False), "0x1")

    @pytest.mark.asyncio
    async def test_missing_node_key_returns_none(self) -> None:
        resp = _resp({})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            assert await find_by_id(_dao(), "some-id") is None


class TestFindOne:
    """Verify find_one returns first match or None."""

    @pytest.mark.asyncio
    async def test_returns_model(self) -> None:
        key = f"{_COLL}_results"
        node = {f"{_COLL}.name": "gamma", f"{_COLL}.app_uid": "g1"}
        resp = _resp({key: [node]})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            result = await find_one(_dao(), {"name": "gamma"})
        assert result is not None
        assert result.name == "gamma"

    @pytest.mark.asyncio
    async def test_empty_returns_none(self) -> None:
        key = f"{_COLL}_results"
        resp = _resp({key: []})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            assert await find_one(_dao(), {"name": "x"}) is None

    @pytest.mark.asyncio
    async def test_not_connected_raises(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await find_one(_dao(connected=False), {"name": "x"})

    @pytest.mark.asyncio
    async def test_missing_key_returns_none(self) -> None:
        resp = _resp({})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            assert await find_one(_dao(), {"name": "z"}) is None


class TestFind:
    """Verify find returns a list of model instances."""

    @pytest.mark.asyncio
    async def test_returns_list(self) -> None:
        key = f"{_COLL}_results"
        nodes = [
            {f"{_COLL}.name": "a", f"{_COLL}.app_uid": "u1"},
            {f"{_COLL}.name": "b", f"{_COLL}.app_uid": "u2"},
        ]
        resp = _resp({key: nodes})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            result = await find(_dao(), {"name": "a"}, limit=10)
        assert len(result) == _TWO
        assert result[0].name == "a"

    @pytest.mark.asyncio
    async def test_empty_returns_empty_list(self) -> None:
        key = f"{_COLL}_results"
        resp = _resp({key: []})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            assert await find(_dao(), {}) == []

    @pytest.mark.asyncio
    async def test_not_connected_raises(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await find(_dao(connected=False), {})

    @pytest.mark.asyncio
    async def test_missing_key_returns_empty(self) -> None:
        resp = _resp({})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            assert await find(_dao(), {"x": "y"}) == []


class TestCount:
    """Verify count extracts the total from Dgraph response."""

    @pytest.mark.asyncio
    async def test_returns_count(self) -> None:
        resp = _resp({"count": [{"total": _COUNT_42}]})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            assert await count(_dao(), {"active": True}) == _COUNT_42

    @pytest.mark.asyncio
    async def test_missing_total_returns_zero(self) -> None:
        resp = _resp({"count": [{}]})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            assert await count(_dao(), {}) == _ZERO

    @pytest.mark.asyncio
    async def test_not_connected_raises(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await count(_dao(connected=False), {})

    @pytest.mark.asyncio
    async def test_none_total_returns_zero(self) -> None:
        resp = _resp({"count": [{"total": None}]})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            assert await count(_dao(), {}) == _ZERO

    @pytest.mark.asyncio
    async def test_empty_count_array_raises(self) -> None:
        resp = _resp({"count": []})
        with (
            patch(_QWT, new_callable=AsyncMock, return_value=resp),
            pytest.raises(IndexError),
        ):
            await count(_dao(), {})


class TestExists:
    """Verify exists for both UID and app-uid checks."""

    @pytest.mark.asyncio
    async def test_uid_exists_true(self) -> None:
        node = {
            "uid": "0x1",
            "dgraph.type": [_COLL],
            f"{_COLL}.name": "x",
        }
        resp = _resp({"node": [node]})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            assert await exists(_dao(), "0x1") is True

    @pytest.mark.asyncio
    async def test_uid_not_found_false(self) -> None:
        resp = _resp({"node": []})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            assert await exists(_dao(), "0xmissing") is False

    @pytest.mark.asyncio
    async def test_app_uid_exists_true(self) -> None:
        resp = _resp({"node": [{"uid": "0x5"}]})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            assert await exists(_dao(), "regular-id") is True

    @pytest.mark.asyncio
    async def test_app_uid_not_found_falsy(self) -> None:
        resp = _resp({"node": []})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            assert not await exists(_dao(), "no-such-id")

    @pytest.mark.asyncio
    async def test_not_connected_raises(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await exists(_dao(connected=False), "0x1")

    @pytest.mark.asyncio
    async def test_query_error_wraps(self) -> None:
        with (
            patch(
                _QWT,
                new_callable=AsyncMock,
                side_effect=RuntimeError("conn lost"),
            ),
            pytest.raises(StorageError, match="Failed to check"),
        ):
            await exists(_dao(), "app-id-123")


class TestRawReadQuery:
    """Verify raw_read_query returns parsed JSON results."""

    @pytest.mark.asyncio
    async def test_dict_wrapped_in_list(self) -> None:
        resp = _resp({"foo": "bar"})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            assert await raw_read_query(_dao(), "q") == [{"foo": "bar"}]

    @pytest.mark.asyncio
    async def test_list_returned_directly(self) -> None:
        payload = [{"a": 1}, {"b": 2}]
        resp = _resp(payload)
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            assert await raw_read_query(_dao(), "q") == payload

    @pytest.mark.asyncio
    async def test_not_connected_raises(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await raw_read_query(_dao(connected=False), "q")

    @pytest.mark.asyncio
    async def test_with_params(self) -> None:
        resp = _resp({"data": "ok"})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp) as m:
            await raw_read_query(
                _dao(), "query find($x: string) {}", params={"$x": "v"}
            )
        assert m.call_args[1]["variables"] == {"$x": "v"}


class TestListDatabases:
    """Verify list_databases returns static default."""

    @pytest.mark.asyncio
    async def test_returns_default(self) -> None:
        assert await list_databases(_dao()) == ["default"]


class TestListSchemas:
    """Verify list_schemas extracts type names."""

    @pytest.mark.asyncio
    async def test_extracts_types(self) -> None:
        data = {
            "types": [
                {"@groupby": [{"dgraph.type": "Person"}]},
                {"@groupby": [{"dgraph.type": "Place"}]},
            ],
        }
        resp = _resp(data)
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            result = await list_schemas(_dao())
        assert "Person" in result
        assert "Place" in result

    @pytest.mark.asyncio
    async def test_empty_returns_empty(self) -> None:
        resp = _resp({"types": []})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            assert await list_schemas(_dao()) == []

    @pytest.mark.asyncio
    async def test_not_connected_raises(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await list_schemas(_dao(connected=False))


class TestListModels:
    """Verify list_models delegates to list_schemas."""

    @pytest.mark.asyncio
    async def test_delegates(self) -> None:
        data = {"types": [{"@groupby": [{"dgraph.type": "W"}]}]}
        resp = _resp(data)
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            assert await list_models(_dao(), "default") == ["W"]


class TestGetModelInfo:
    """Verify get_model_info returns type and count."""

    @pytest.mark.asyncio
    async def test_returns_info(self) -> None:
        data = {"type_info": [{"count(uid)": _COUNT_42}]}
        resp = _resp(data)
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            r = await get_model_info(_dao(), "Person")
        assert r["type"] == "Person"
        assert r["count"] == _COUNT_42

    @pytest.mark.asyncio
    async def test_not_connected_raises(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await get_model_info(_dao(connected=False), "X")


class TestGetModelSchema:
    """Verify get_model_schema filters predicates."""

    @pytest.mark.asyncio
    async def test_filters_by_prefix(self) -> None:
        data = {
            "schema": [
                {"predicate": "Person.name", "type": "string"},
                {"predicate": "Place.city", "type": "string"},
            ],
        }
        resp = _resp(data)
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            r = await get_model_schema(_dao(), "Person")
        assert "Person.name" in r
        assert "Place.city" not in r

    @pytest.mark.asyncio
    async def test_not_connected_raises(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await get_model_schema(_dao(connected=False), "X")


class TestGetModelFields:
    """Verify get_model_fields builds field list."""

    @pytest.mark.asyncio
    async def test_returns_fields(self) -> None:
        data = {
            "schema": [
                {"predicate": "P.name", "type": "string", "index": True},
                {"predicate": "P.age", "type": "int"},
            ],
        }
        resp = _resp(data)
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            result = await get_model_fields(_dao(), "P")
        names = [f["name"] for f in result]
        assert "name" in names
        assert "age" in names


class TestGetModelIndexes:
    """Verify get_model_indexes filters indexed fields."""

    @pytest.mark.asyncio
    async def test_returns_only_indexed(self) -> None:
        data = {
            "schema": [
                {"predicate": "P.name", "type": "string", "index": True},
                {"predicate": "P.bio", "type": "string"},
            ],
        }
        resp = _resp(data)
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            result = await get_model_indexes(_dao(), "P")
        assert len(result) == _ONE
        assert result[0]["field"] == "name"


class TestCheckConnection:
    """Verify the connection health-check function."""

    @pytest.mark.asyncio
    async def test_healthy_returns_true(self) -> None:
        resp = _resp({"schema": []})
        with patch(_QWT, new_callable=AsyncMock, return_value=resp):
            assert await _check_conn(_dao()) is True

    @pytest.mark.asyncio
    async def test_not_initialized_raises(self) -> None:
        with pytest.raises(StorageError, match="Client not initialized"):
            await _check_conn(_dao(connected=False))

    @pytest.mark.asyncio
    async def test_query_failure_raises(self) -> None:
        with (
            patch(
                _QWT,
                new_callable=AsyncMock,
                side_effect=RuntimeError("timeout"),
            ),
            pytest.raises(StorageError, match="Health check"),
        ):
            await _check_conn(_dao())
