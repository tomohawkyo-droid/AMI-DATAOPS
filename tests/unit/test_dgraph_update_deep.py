"""Async-function tests for dgraph_update operations.

Covers _get_actual_uid, update, bulk_update, and raw_write_query
with mocked pydgraph transactions.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.core.exceptions import StorageError, StorageValidationError
from ami.implementations.graph.dgraph_update import (
    _get_actual_uid,
    _parse_uid_response,
    _prepare_delete_data,
    _prepare_update_data,
    bulk_update,
    raw_write_query,
    update,
)

# -- constants -------------------------------------------------------

COLLECTION = "widgets"
ITEM_ID = "item-abc-123"
HEX_UID = "0x2a"
BULK_COUNT = 3
EXPECTED_ONE = 1
EXPECTED_TWO = 2
SECOND_CALL = 2
_MOD = "ami.implementations.graph.dgraph_update"


# -- helpers ---------------------------------------------------------


def _make_dao(
    collection_name: str = COLLECTION,
    connected: bool = True,
) -> MagicMock:
    """Build a minimal DAO mock with client and txn."""
    dao = MagicMock()
    dao.collection_name = collection_name
    if connected:
        txn = MagicMock()
        txn.query = MagicMock()
        txn.mutate = MagicMock()
        txn.commit = MagicMock()
        txn.discard = MagicMock()
        dao.client = MagicMock()
        dao.client.txn.return_value = txn
    else:
        dao.client = None
    return dao


def _uid_json(uid: str = HEX_UID) -> str:
    return json.dumps({"node": [{"uid": uid}]})


# -- _parse_uid_response ---------------------------------------------


class TestParseUidResponse:
    """Cover every branch in _parse_uid_response."""

    def test_valid_response(self) -> None:
        result = _parse_uid_response(_uid_json(), ITEM_ID)
        assert result == HEX_UID

    def test_invalid_json_raises(self) -> None:
        with pytest.raises(StorageError, match="parse Dgraph"):
            _parse_uid_response("{bad", ITEM_ID)

    def test_non_dict_top_level(self) -> None:
        with pytest.raises(StorageError, match="expected dict"):
            _parse_uid_response(json.dumps([1, 2]), ITEM_ID)

    def test_missing_node_key(self) -> None:
        with pytest.raises(StorageError, match="not found"):
            _parse_uid_response(json.dumps({"other": 1}), ITEM_ID)

    def test_empty_node_list(self) -> None:
        with pytest.raises(StorageError, match="not found"):
            _parse_uid_response(json.dumps({"node": []}), ITEM_ID)

    def test_node_not_a_list(self) -> None:
        with pytest.raises(StorageError, match="must be list"):
            _parse_uid_response(json.dumps({"node": "oops"}), ITEM_ID)

    def test_uid_missing_in_node(self) -> None:
        with pytest.raises(StorageError, match="UID missing"):
            _parse_uid_response(
                json.dumps({"node": [{"name": "x"}]}),
                ITEM_ID,
            )

    def test_uid_not_a_string(self) -> None:
        with pytest.raises(StorageError, match="Invalid UID type"):
            _parse_uid_response(
                json.dumps({"node": [{"uid": 42}]}),
                ITEM_ID,
            )


# -- _prepare_delete_data / _prepare_update_data ---------------------


class TestPrepareDeleteData:
    """Cover _prepare_delete_data helper."""

    def test_basic_delete_data(self) -> None:
        dao = _make_dao()
        result = _prepare_delete_data(dao, HEX_UID, {"title": "x", "count": 1})
        assert result["uid"] == HEX_UID
        assert result[f"{COLLECTION}.title"] is None
        assert result[f"{COLLECTION}.count"] is None

    def test_id_field_skipped(self) -> None:
        dao = _make_dao()
        result = _prepare_delete_data(dao, HEX_UID, {"id": "skip-me", "name": "v"})
        assert f"{COLLECTION}.id" not in result
        assert result[f"{COLLECTION}.name"] is None


class TestPrepareUpdateData:
    """Cover _prepare_update_data helper."""

    def test_basic_update_data(self) -> None:
        dao = _make_dao()
        result = _prepare_update_data(dao, HEX_UID, {"title": "new"})
        assert result["uid"] == HEX_UID
        assert result[f"{COLLECTION}.title"] == "new"

    def test_uid_key_maps_to_app_uid(self) -> None:
        dao = _make_dao()
        result = _prepare_update_data(dao, HEX_UID, {"uid": "u1"})
        assert result[f"{COLLECTION}.app_uid"] == "u1"

    def test_none_values_skipped(self) -> None:
        dao = _make_dao()
        result = _prepare_update_data(dao, HEX_UID, {"title": None, "name": "kept"})
        assert f"{COLLECTION}.title" not in result
        assert result[f"{COLLECTION}.name"] == "kept"

    def test_id_field_skipped(self) -> None:
        dao = _make_dao()
        result = _prepare_update_data(dao, HEX_UID, {"id": "skip", "name": "v"})
        assert f"{COLLECTION}.id" not in result


# -- _get_actual_uid -------------------------------------------------


class TestGetActualUid:
    """Async tests for _get_actual_uid."""

    @pytest.mark.asyncio
    async def test_hex_id_returned_directly(self) -> None:
        dao = _make_dao()
        result = await _get_actual_uid(dao, HEX_UID)
        assert result == HEX_UID

    @pytest.mark.asyncio
    async def test_queries_dgraph(self) -> None:
        dao = _make_dao()
        resp = MagicMock()
        resp.json = _uid_json("0xff")
        txn = dao.client.txn.return_value
        with patch(
            f"{_MOD}.query_with_timeout",
            new_callable=AsyncMock,
            return_value=resp,
        ):
            result = await _get_actual_uid(dao, ITEM_ID)
        assert result == "0xff"
        txn.discard.assert_called_once()

    @pytest.mark.asyncio
    async def test_query_failure_wraps_error(self) -> None:
        dao = _make_dao()
        with (
            patch(
                f"{_MOD}.query_with_timeout",
                new_callable=AsyncMock,
                side_effect=RuntimeError("timeout"),
            ),
            pytest.raises(StorageError, match="Failed to execute"),
        ):
            await _get_actual_uid(dao, ITEM_ID)

    @pytest.mark.asyncio
    async def test_invalid_item_id_raises(self) -> None:
        dao = _make_dao()
        with pytest.raises(StorageValidationError, match="cannot be empty"):
            await _get_actual_uid(dao, "")


# -- update ----------------------------------------------------------


class TestUpdate:
    """Async tests for the update function."""

    @pytest.mark.asyncio
    async def test_not_connected_raises(self) -> None:
        dao = _make_dao(connected=False)
        with pytest.raises(StorageError, match="Not connected"):
            await update(dao, ITEM_ID, {"title": "x"})

    @pytest.mark.asyncio
    async def test_successful_update(self) -> None:
        dao = _make_dao()
        with (
            patch(
                f"{_MOD}._get_actual_uid",
                new_callable=AsyncMock,
                return_value=HEX_UID,
            ),
            patch(
                f"{_MOD}.mutate_with_timeout",
                new_callable=AsyncMock,
            ) as mock_mut,
            patch(
                f"{_MOD}.commit_with_timeout",
                new_callable=AsyncMock,
            ) as mock_cmt,
        ):
            await update(dao, ITEM_ID, {"title": "new"})
        assert mock_mut.await_count == EXPECTED_TWO
        mock_cmt.assert_awaited_once()
        txn = dao.client.txn.return_value
        txn.discard.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_mutation_failure(self) -> None:
        dao = _make_dao()
        with (
            patch(
                f"{_MOD}._get_actual_uid",
                new_callable=AsyncMock,
                return_value=HEX_UID,
            ),
            patch(
                f"{_MOD}.mutate_with_timeout",
                new_callable=AsyncMock,
                side_effect=RuntimeError("del fail"),
            ),
            pytest.raises(StorageError, match="delete mutation"),
        ):
            await update(dao, ITEM_ID, {"title": "x"})

    @pytest.mark.asyncio
    async def test_set_mutation_failure(self) -> None:
        dao = _make_dao()
        call_count = 0

        async def _side(*a: Any, **kw: Any) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == SECOND_CALL:
                msg = "set fail"
                raise RuntimeError(msg)

        with (
            patch(
                f"{_MOD}._get_actual_uid",
                new_callable=AsyncMock,
                return_value=HEX_UID,
            ),
            patch(
                f"{_MOD}.mutate_with_timeout",
                new_callable=AsyncMock,
                side_effect=_side,
            ),
            pytest.raises(StorageError, match="set mutation"),
        ):
            await update(dao, ITEM_ID, {"title": "x"})

    @pytest.mark.asyncio
    async def test_commit_failure(self) -> None:
        dao = _make_dao()
        with (
            patch(
                f"{_MOD}._get_actual_uid",
                new_callable=AsyncMock,
                return_value=HEX_UID,
            ),
            patch(
                f"{_MOD}.mutate_with_timeout",
                new_callable=AsyncMock,
            ),
            patch(
                f"{_MOD}.commit_with_timeout",
                new_callable=AsyncMock,
                side_effect=RuntimeError("commit fail"),
            ),
            pytest.raises(StorageError, match="commit transaction"),
        ):
            await update(dao, ITEM_ID, {"title": "x"})

    @pytest.mark.asyncio
    async def test_discard_called_on_error(self) -> None:
        dao = _make_dao()
        with (
            patch(
                f"{_MOD}._get_actual_uid",
                new_callable=AsyncMock,
                return_value=HEX_UID,
            ),
            patch(
                f"{_MOD}.mutate_with_timeout",
                new_callable=AsyncMock,
                side_effect=RuntimeError("boom"),
            ),
            pytest.raises(StorageError),
        ):
            await update(dao, ITEM_ID, {"title": "x"})
        txn = dao.client.txn.return_value
        txn.discard.assert_called()


# -- bulk_update -----------------------------------------------------


class TestBulkUpdate:
    """Async tests for bulk_update."""

    @pytest.mark.asyncio
    async def test_not_connected_raises(self) -> None:
        dao = _make_dao(connected=False)
        with pytest.raises(StorageError, match="Not connected"):
            await bulk_update(dao, [{"uid": "x"}])

    @pytest.mark.asyncio
    async def test_missing_uid_and_id(self) -> None:
        dao = _make_dao()
        with pytest.raises(StorageValidationError, match="missing required"):
            await bulk_update(dao, [{"title": "no id"}])

    @pytest.mark.asyncio
    async def test_successful_bulk(self) -> None:
        dao = _make_dao()
        items = [
            {"uid": "a1", "title": "one"},
            {"uid": "a2", "title": "two"},
            {"uid": "a3", "title": "three"},
        ]
        with patch(
            f"{_MOD}.update",
            new_callable=AsyncMock,
        ) as mock_upd:
            await bulk_update(dao, items)
        assert mock_upd.await_count == BULK_COUNT

    @pytest.mark.asyncio
    async def test_uses_id_when_uid_absent(self) -> None:
        dao = _make_dao()
        items = [{"id": "id1", "title": "one"}]
        with patch(
            f"{_MOD}.update",
            new_callable=AsyncMock,
        ) as mock_upd:
            await bulk_update(dao, items)
        mock_upd.assert_awaited_once_with(dao, "id1", items[0])

    @pytest.mark.asyncio
    async def test_partial_failures_aggregated(self) -> None:
        dao = _make_dao()
        items = [
            {"uid": "ok1", "title": "fine"},
            {"uid": "bad1", "title": "fail1"},
            {"uid": "bad2", "title": "fail2"},
        ]

        async def _fail(d: Any, uid: str, data: Any) -> None:
            if uid.startswith("bad"):
                msg = f"err-{uid}"
                raise StorageError(msg)

        with (
            patch(
                f"{_MOD}.update",
                new_callable=AsyncMock,
                side_effect=_fail,
            ),
            pytest.raises(StorageError, match="Bulk update failed"),
        ):
            await bulk_update(dao, items)


# -- raw_write_query -------------------------------------------------


class TestRawWriteQuery:
    """Async tests for raw_write_query."""

    @pytest.mark.asyncio
    async def test_params_not_supported(self) -> None:
        dao = _make_dao()
        with pytest.raises(NotImplementedError):
            await raw_write_query(
                dao,
                "<_:a> <name> 'v' .",
                params={"k": "v"},
            )

    @pytest.mark.asyncio
    async def test_not_connected_raises(self) -> None:
        dao = _make_dao(connected=False)
        with pytest.raises(StorageError, match="Not connected"):
            await raw_write_query(dao, "<_:a> <name> 'v' .")

    @pytest.mark.asyncio
    async def test_empty_query_raises(self) -> None:
        dao = _make_dao()
        with pytest.raises(StorageValidationError, match="cannot be empty"):
            await raw_write_query(dao, "")

    @pytest.mark.asyncio
    async def test_successful_write(self) -> None:
        dao = _make_dao()
        resp = MagicMock()
        resp.uids = {"blank-0": "0x1"}
        with (
            patch(
                f"{_MOD}.mutate_with_timeout",
                new_callable=AsyncMock,
                return_value=resp,
            ),
            patch(
                f"{_MOD}.commit_with_timeout",
                new_callable=AsyncMock,
            ),
        ):
            count = await raw_write_query(dao, "<_:a> <name> 'val' .")
        assert count == EXPECTED_ONE

    @pytest.mark.asyncio
    async def test_mutation_failure(self) -> None:
        dao = _make_dao()
        with (
            patch(
                f"{_MOD}.mutate_with_timeout",
                new_callable=AsyncMock,
                side_effect=RuntimeError("network"),
            ),
            pytest.raises(StorageError, match="execute mutation"),
        ):
            await raw_write_query(dao, "<_:a> <name> 'v' .")

    @pytest.mark.asyncio
    async def test_commit_failure(self) -> None:
        dao = _make_dao()
        resp = MagicMock()
        resp.uids = {}
        with (
            patch(
                f"{_MOD}.mutate_with_timeout",
                new_callable=AsyncMock,
                return_value=resp,
            ),
            patch(
                f"{_MOD}.commit_with_timeout",
                new_callable=AsyncMock,
                side_effect=RuntimeError("commit err"),
            ),
            pytest.raises(StorageError, match="commit transaction"),
        ):
            await raw_write_query(dao, "<_:a> <name> 'v' .")

    @pytest.mark.asyncio
    async def test_discard_called_on_success(self) -> None:
        dao = _make_dao()
        resp = MagicMock()
        resp.uids = {}
        with (
            patch(
                f"{_MOD}.mutate_with_timeout",
                new_callable=AsyncMock,
                return_value=resp,
            ),
            patch(
                f"{_MOD}.commit_with_timeout",
                new_callable=AsyncMock,
            ),
        ):
            await raw_write_query(dao, "<_:a> <name> 'v' .")
        txn = dao.client.txn.return_value
        txn.discard.assert_called_once()

    @pytest.mark.asyncio
    async def test_discard_called_on_error(self) -> None:
        dao = _make_dao()
        with (
            patch(
                f"{_MOD}.mutate_with_timeout",
                new_callable=AsyncMock,
                side_effect=RuntimeError("boom"),
            ),
            pytest.raises(StorageError),
        ):
            await raw_write_query(dao, "<_:a> <name> 'v' .")
        txn = dao.client.txn.return_value
        txn.discard.assert_called_once()
