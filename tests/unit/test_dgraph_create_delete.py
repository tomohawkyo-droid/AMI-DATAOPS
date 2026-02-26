"""Tests for dgraph_create and dgraph_delete module-level functions.

Exercises the actual async functions with mocked pydgraph
transactions and clients, covering success and error branches.
"""

from __future__ import annotations

import json
from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

import pytest

from ami.core.exceptions import StorageError, StorageValidationError
from ami.implementations.graph.dgraph_create import (
    bulk_create,
    create,
    create_indexes,
)
from ami.implementations.graph.dgraph_delete import (
    bulk_delete,
    delete,
)
from ami.models.base_model import ModelMetadata, StorageModel

# -----------------------------------------------------------------
# Constants (PLR2004)
# -----------------------------------------------------------------

_BULK_SIZE = 3
_EXPECTED_BULK_SUCCESS = 2
_EXPECTED_BULK_FAILED = 1
_EXPECTED_TOTAL = 3


# -----------------------------------------------------------------
# Test model
# -----------------------------------------------------------------


class _SampleModel(StorageModel):
    """Minimal model used across all tests in this module."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="samples",
    )
    name: str = "default"


# -----------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------


def _make_dao(
    *,
    connected: bool = True,
    collection_name: str = "samples",
) -> MagicMock:
    """Build a mock DAO object with a mock client."""
    dao = MagicMock()
    dao.collection_name = collection_name
    dao.model_cls = _SampleModel
    if connected:
        dao.client = MagicMock()
    else:
        dao.client = None
    return dao


def _make_txn(
    *,
    mutate_uids: dict[str, str] | None = None,
    query_json: str | None = None,
) -> MagicMock:
    """Build a mock transaction.

    Parameters
    ----------
    mutate_uids:
        Dictionary returned via ``response.uids`` after mutate.
    query_json:
        JSON string returned via ``response.json`` after query.
    """
    txn = MagicMock()

    # mutate response
    mutate_resp = MagicMock()
    mutate_resp.uids = mutate_uids or {}
    txn.mutate.return_value = mutate_resp

    # query response
    query_resp = MagicMock()
    query_resp.json = query_json or "{}"
    txn.query.return_value = query_resp

    return txn


def _instance_with_uid(uid: str = "app-uid-1") -> _SampleModel:
    """Return a model instance with a known uid."""
    return _SampleModel(uid=uid, name="test-node")


# =================================================================
# dgraph_create.create
# =================================================================


class TestCreate:
    """Tests for dgraph_create.create."""

    @pytest.mark.asyncio
    async def test_success_returns_app_uid(self) -> None:
        """Successful create returns the application uid."""
        dao = _make_dao()
        txn = _make_txn(mutate_uids={"blank-0": "0xf1"})
        dao.client.txn.return_value = txn

        inst = _instance_with_uid("my-app-uid")
        result = await create(dao, inst)

        assert result == "my-app-uid"
        txn.mutate.assert_called_once()
        txn.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_client_raises(self) -> None:
        """Raises StorageError when client is None."""
        dao = _make_dao(connected=False)
        inst = _instance_with_uid()
        with pytest.raises(StorageError, match="Not connected"):
            await create(dao, inst)

    @pytest.mark.asyncio
    async def test_no_uid_on_instance_raises(self) -> None:
        """Raises StorageError when instance has no uid."""
        dao = _make_dao()
        inst = _SampleModel(uid=None, name="no-uid")
        with pytest.raises(StorageError, match="must have a uid"):
            await create(dao, inst)

    @pytest.mark.asyncio
    async def test_no_uid_in_response_raises(self) -> None:
        """Raises StorageError when Dgraph returns no uid."""
        dao = _make_dao()
        txn = _make_txn(mutate_uids={})
        dao.client.txn.return_value = txn

        inst = _instance_with_uid()
        with pytest.raises(StorageError, match="Failed to get UID"):
            await create(dao, inst)

    @pytest.mark.asyncio
    async def test_mutate_exception_discards_txn(self) -> None:
        """Exception during mutate discards the transaction."""
        dao = _make_dao()
        txn = _make_txn()
        txn.mutate.side_effect = RuntimeError("grpc down")
        dao.client.txn.return_value = txn

        with pytest.raises(StorageError, match="Failed to create"):
            await create(dao, _instance_with_uid())

        txn.discard.assert_called_once()

    @pytest.mark.asyncio
    async def test_commit_exception_discards_txn(self) -> None:
        """Exception during commit discards the transaction."""
        dao = _make_dao()
        txn = _make_txn(mutate_uids={"blank-0": "0x1"})
        txn.commit.side_effect = RuntimeError("commit failed")
        dao.client.txn.return_value = txn

        with pytest.raises(StorageError, match="Failed to create"):
            await create(dao, _instance_with_uid())

        txn.discard.assert_called_once()


# =================================================================
# dgraph_create.bulk_create
# =================================================================


class TestBulkCreate:
    """Tests for dgraph_create.bulk_create."""

    @pytest.mark.asyncio
    async def test_success_returns_app_uids(self) -> None:
        """Returns application uids for all created nodes."""
        dao = _make_dao()
        txn = _make_txn(
            mutate_uids={
                "blank-0": "0xa",
                "blank-1": "0xb",
                "blank-2": "0xc",
            },
        )
        dao.client.txn.return_value = txn

        instances = [
            _SampleModel(uid=f"uid-{i}", name=f"n{i}") for i in range(_BULK_SIZE)
        ]
        result = await bulk_create(dao, instances)

        assert result == ["uid-0", "uid-1", "uid-2"]
        txn.mutate.assert_called_once()
        txn.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_client_raises(self) -> None:
        """Raises StorageError when client is None."""
        dao = _make_dao(connected=False)
        instances = [_instance_with_uid()]
        with pytest.raises(StorageError, match="Not connected"):
            await bulk_create(dao, instances)

    @pytest.mark.asyncio
    async def test_missing_uid_on_instance_raises(self) -> None:
        """Raises StorageError if any instance lacks a uid."""
        dao = _make_dao()
        instances = [
            _SampleModel(uid="ok-uid", name="ok"),
            _SampleModel(uid=None, name="no-uid"),
        ]
        with pytest.raises(StorageError, match="must have a uid"):
            await bulk_create(dao, instances)

    @pytest.mark.asyncio
    async def test_mutate_exception_discards_txn(self) -> None:
        """Exception during mutate discards the transaction."""
        dao = _make_dao()
        txn = _make_txn()
        txn.mutate.side_effect = RuntimeError("network error")
        dao.client.txn.return_value = txn

        instances = [_instance_with_uid()]
        with pytest.raises(StorageError, match="Failed to bulk create"):
            await bulk_create(dao, instances)

        txn.discard.assert_called_once()

    @pytest.mark.asyncio
    async def test_partial_uids_logs_warning(self) -> None:
        """Missing uid for some nodes logs a warning but succeeds."""
        dao = _make_dao()
        # Only return uid for blank-0, not blank-1
        txn = _make_txn(mutate_uids={"blank-0": "0xa"})
        dao.client.txn.return_value = txn

        instances = [
            _SampleModel(uid="uid-0", name="a"),
            _SampleModel(uid="uid-1", name="b"),
        ]
        result = await bulk_create(dao, instances)

        # Still returns all app uids even if Dgraph missed some
        assert result == ["uid-0", "uid-1"]


# =================================================================
# dgraph_create.create_indexes
# =================================================================


class TestCreateIndexes:
    """Tests for dgraph_create.create_indexes."""

    @pytest.mark.asyncio
    async def test_calls_ensure_schema_with_metadata(self) -> None:
        """Calls ensure_schema when model has get_metadata."""
        dao = _make_dao()
        with patch(
            "ami.implementations.graph.dgraph_create.ensure_schema",
        ) as mock_schema:
            await create_indexes(dao)

        mock_schema.assert_called_once_with(
            dao.client,
            _SampleModel,
            _SampleModel.get_metadata(),
            "samples",
        )

    @pytest.mark.asyncio
    async def test_calls_ensure_schema_no_metadata(self) -> None:
        """Passes None when model lacks get_metadata."""
        dao = _make_dao()
        # Use a model class that has no get_metadata
        dao.model_cls = MagicMock(spec=[])
        with patch(
            "ami.implementations.graph.dgraph_create.ensure_schema",
        ) as mock_schema:
            await create_indexes(dao)

        mock_schema.assert_called_once_with(
            dao.client,
            dao.model_cls,
            None,
            "samples",
        )


# =================================================================
# dgraph_delete.delete
# =================================================================


class TestDelete:
    """Tests for dgraph_delete.delete."""

    @pytest.mark.asyncio
    async def test_delete_with_dgraph_uid(self) -> None:
        """Direct deletion when item_id starts with 0x."""
        dao = _make_dao()
        txn = _make_txn()
        dao.client.txn.return_value = txn

        result = await delete(dao, "0xabc")

        assert result is True
        txn.mutate.assert_called_once()
        txn.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_with_app_uid_found(self) -> None:
        """Looks up Dgraph uid when item_id is an app uid."""
        dao = _make_dao()

        # read-only txn for lookup
        lookup_txn = _make_txn(
            query_json=json.dumps(
                {"node": [{"uid": "0xfound"}]},
            ),
        )
        # write txn for deletion
        write_txn = _make_txn()

        dao.client.txn.side_effect = [lookup_txn, write_txn]

        result = await delete(dao, "app-uid-123")

        assert result is True
        lookup_txn.query.assert_called_once()
        write_txn.mutate.assert_called_once()
        write_txn.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_app_uid_not_found(self) -> None:
        """Returns False when app uid is not found in Dgraph."""
        dao = _make_dao()

        lookup_txn = _make_txn(
            query_json=json.dumps({"node": []}),
        )
        dao.client.txn.return_value = lookup_txn

        result = await delete(dao, "nonexistent-uid")

        assert result is False
        lookup_txn.discard.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_app_uid_empty_node(self) -> None:
        """Returns False when 'node' key is missing."""
        dao = _make_dao()

        lookup_txn = _make_txn(
            query_json=json.dumps({}),
        )
        dao.client.txn.return_value = lookup_txn

        result = await delete(dao, "missing-node-key")

        assert result is False

    @pytest.mark.asyncio
    async def test_no_client_raises(self) -> None:
        """Raises StorageError when client is None."""
        dao = _make_dao(connected=False)
        with pytest.raises(StorageError, match="Not connected"):
            await delete(dao, "0x1")

    @pytest.mark.asyncio
    async def test_invalid_collection_name_raises(self) -> None:
        """Raises StorageValidationError for bad collection name."""
        dao = _make_dao(collection_name="bad--name")
        with pytest.raises(StorageValidationError):
            await delete(dao, "0x1")

    @pytest.mark.asyncio
    async def test_mutate_exception_raises_storage_error(self) -> None:
        """Exception during delete mutation wraps in StorageError."""
        dao = _make_dao()
        txn = _make_txn()
        txn.mutate.side_effect = RuntimeError("grpc fail")
        dao.client.txn.return_value = txn

        with pytest.raises(StorageError, match="Failed to delete"):
            await delete(dao, "0xabc")

    @pytest.mark.asyncio
    async def test_query_exception_wraps_in_storage_error(self) -> None:
        """Exception during uid lookup wraps in StorageError."""
        dao = _make_dao()
        lookup_txn = _make_txn()
        lookup_txn.query.side_effect = RuntimeError("query failed")
        dao.client.txn.return_value = lookup_txn

        with pytest.raises(StorageError, match="Failed to query"):
            await delete(dao, "app-uid-error")

    @pytest.mark.asyncio
    async def test_delete_discards_txn_on_failure(self) -> None:
        """Write transaction is discarded even on failure."""
        dao = _make_dao()
        txn = _make_txn()
        txn.mutate.side_effect = RuntimeError("boom")
        dao.client.txn.return_value = txn

        with pytest.raises(StorageError):
            await delete(dao, "0x1")

        txn.discard.assert_called()


# =================================================================
# dgraph_delete.bulk_delete
# =================================================================


class TestBulkDelete:
    """Tests for dgraph_delete.bulk_delete."""

    @pytest.mark.asyncio
    async def test_all_succeed(self) -> None:
        """All deletions succeed."""
        dao = _make_dao()
        txn = _make_txn()
        dao.client.txn.return_value = txn

        ids = ["0x1", "0x2", "0x3"]
        result = await bulk_delete(dao, ids)

        assert result["success_count"] == _EXPECTED_TOTAL
        assert result["failed_ids"] == []
        assert result["total"] == _EXPECTED_TOTAL

    @pytest.mark.asyncio
    async def test_partial_failures(self) -> None:
        """Tracks failed deletions separately."""
        dao = _make_dao()

        call_count = 0

        async def _side_effect(d: Any, uid: str) -> bool:
            nonlocal call_count
            call_count += 1
            if uid == "0x2":
                return False
            if uid == "0x3":
                msg = "delete error"
                raise RuntimeError(msg)
            return True

        ids = ["0x1", "0x2", "0x3"]
        with patch(
            "ami.implementations.graph.dgraph_delete.delete",
            side_effect=_side_effect,
        ):
            result = await bulk_delete(dao, ids)

        assert result["success_count"] == 1
        assert "0x2" in result["failed_ids"]
        assert "0x3" in result["failed_ids"]
        assert len(result["failed_ids"]) == _EXPECTED_BULK_FAILED + 1
        assert result["total"] == _EXPECTED_TOTAL

    @pytest.mark.asyncio
    async def test_empty_ids_list(self) -> None:
        """Empty id list returns zero counts."""
        dao = _make_dao()
        result = await bulk_delete(dao, [])

        assert result["success_count"] == 0
        assert result["failed_ids"] == []
        assert result["total"] == 0

    @pytest.mark.asyncio
    async def test_all_fail(self) -> None:
        """All deletions fail."""
        dao = _make_dao()

        async def _always_fail(d: Any, uid: str) -> bool:
            msg = "always fails"
            raise RuntimeError(msg)

        ids = ["0x1", "0x2"]
        with patch(
            "ami.implementations.graph.dgraph_delete.delete",
            side_effect=_always_fail,
        ):
            result = await bulk_delete(dao, ids)

        assert result["success_count"] == 0
        assert len(result["failed_ids"]) == _EXPECTED_BULK_SUCCESS
        assert result["total"] == _EXPECTED_BULK_SUCCESS
