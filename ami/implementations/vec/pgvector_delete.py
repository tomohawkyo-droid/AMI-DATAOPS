"""PgVector DELETE operations."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ami.core.exceptions import StorageConnectionError
from ami.implementations.sql.postgresql_util import parse_affected_count
from ami.implementations.vec.pgvector_util import get_safe_table_name

if TYPE_CHECKING:
    from ami.implementations.vec.pgvector_dao import PgVectorDAO

logger = logging.getLogger(__name__)


async def delete(dao: PgVectorDAO, item_id: str) -> bool:
    """Delete the record identified by *item_id*. Return success flag."""
    table = get_safe_table_name(dao.collection_name)
    sql = f"DELETE FROM {table} WHERE uid = $1"

    if dao.pool is None:
        msg = "Connection pool not available"
        raise StorageConnectionError(msg)
    async with dao.pool.acquire() as conn:
        result = await conn.execute(sql, item_id)

    deleted = parse_affected_count(result) > 0
    if deleted:
        logger.debug("Deleted record %s from %s", item_id, table)
    else:
        logger.debug("Record %s not found in %s for deletion", item_id, table)
    return bool(deleted)


async def bulk_delete(
    dao: PgVectorDAO,
    ids: list[str],
) -> int:
    """Delete multiple records by *ids*. Return count of deleted rows."""
    if not ids:
        return 0

    table = get_safe_table_name(dao.collection_name)
    params = ", ".join(f"${i + 1}" for i in range(len(ids)))
    sql = f"DELETE FROM {table} WHERE uid IN ({params})"

    if dao.pool is None:
        msg = "Connection pool not available"
        raise StorageConnectionError(msg)
    async with dao.pool.acquire() as conn:
        result = await conn.execute(sql, *ids)

    return parse_affected_count(result)
