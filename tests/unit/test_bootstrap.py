"""Unit tests for ami.bootstrap.register_all_daos()."""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock, patch

# Force-mock pydgraph before any ami imports that pull it in
# transitively via DgraphDAO.
if "pydgraph" not in sys.modules:
    _mod = types.ModuleType("pydgraph")
    _mod.DgraphClient = MagicMock
    _mod.DgraphClientStub = MagicMock
    _mod.Mutation = MagicMock
    _mod.Operation = MagicMock
    sys.modules["pydgraph"] = _mod

from ami.bootstrap import register_all_daos
from ami.core.storage_types import StorageType
from ami.implementations.graph.dgraph_dao import DgraphDAO
from ami.implementations.mem.redis_dao import RedisDAO
from ami.implementations.rest.rest_dao import RestDAO
from ami.implementations.sql.postgresql_dao import PostgreSQLDAO
from ami.implementations.timeseries.prometheus_dao import (
    PrometheusDAO,
)
from ami.implementations.vault.openbao_dao import OpenBaoDAO
from ami.implementations.vec.pgvector_dao import PgVectorDAO

_EXPECTED_DAO_COUNT = 7


class TestRegisterAllDaos:
    """Verify register_all_daos registers every backend."""

    @patch("ami.bootstrap.register_dao")
    def test_called_seven_times(self, mock_register: MagicMock) -> None:
        register_all_daos()
        assert mock_register.call_count == _EXPECTED_DAO_COUNT

    @patch("ami.bootstrap.register_dao")
    def test_registered_pairs(self, mock_register: MagicMock) -> None:
        register_all_daos()

        expected = [
            (StorageType.GRAPH, DgraphDAO),
            (StorageType.INMEM, RedisDAO),
            (StorageType.RELATIONAL, PostgreSQLDAO),
            (StorageType.VECTOR, PgVectorDAO),
            (StorageType.TIMESERIES, PrometheusDAO),
            (StorageType.REST, RestDAO),
            (StorageType.VAULT, OpenBaoDAO),
        ]

        actual = [(call.args[0], call.args[1]) for call in mock_register.call_args_list]

        assert actual == expected
