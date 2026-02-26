"""Register all DAO implementations. Call register_all_daos() at application startup."""

from ami.core.dao import register_dao
from ami.core.storage_types import StorageType
from ami.implementations.graph.dgraph_dao import DgraphDAO
from ami.implementations.mem.redis_dao import RedisDAO
from ami.implementations.rest.rest_dao import RestDAO
from ami.implementations.sql.postgresql_dao import PostgreSQLDAO
from ami.implementations.timeseries.prometheus_dao import PrometheusDAO
from ami.implementations.vault.openbao_dao import OpenBaoDAO
from ami.implementations.vec.pgvector_dao import PgVectorDAO


def register_all_daos() -> None:
    """Register all storage backend implementations with the DAO registry."""
    register_dao(StorageType.GRAPH, DgraphDAO)
    register_dao(StorageType.INMEM, RedisDAO)
    register_dao(StorageType.RELATIONAL, PostgreSQLDAO)
    register_dao(StorageType.VECTOR, PgVectorDAO)
    register_dao(StorageType.TIMESERIES, PrometheusDAO)
    register_dao(StorageType.REST, RestDAO)
    register_dao(StorageType.VAULT, OpenBaoDAO)
