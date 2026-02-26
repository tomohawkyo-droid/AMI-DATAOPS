"""Tests for Prometheus sub-operation async functions."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.core.exceptions import StorageError
from ami.implementations.timeseries.prometheus_connection import (
    check_ready,
    close_session,
    create_session,
    health_check,
)
from ami.implementations.timeseries.prometheus_metadata import (
    get_model_fields,
    get_model_indexes,
    get_model_info,
    get_model_schema,
    list_databases,
    list_models,
    list_schemas,
)
from ami.implementations.timeseries.prometheus_metadata import (
    test_connection as prom_test_connection,
)
from ami.implementations.timeseries.prometheus_read import (
    find_metric_by_labels,
    find_metrics,
    get_alerts,
    get_label_names,
    get_label_values,
    get_metric_metadata,
    get_rules,
    get_series,
    get_targets,
    instant_query,
    range_query,
)
from ami.implementations.timeseries.prometheus_write import (
    clean_tombstones,
    delete_from_gateway,
    delete_series,
    snapshot,
    write_metrics,
    write_single_metric,
)

HTTP_OK = 200
HTTP_NO_CONTENT = 204
HTTP_ACCEPTED = 202
HTTP_BAD = 400
HTTP_ERR = 500
_TWO = 2
_THREE = 3
_TS = 1704067200.0
_B = "http://localhost:9090"
_P = "ami.implementations.timeseries.prometheus_"
_RR = f"{_P}read.request_with_retry"
_RW = f"{_P}write.request_with_retry"
_GW = f"{_P}write._push_to_gateway"
_M = f"{_P}metadata"
_T1 = datetime(2024, 1, 1, tzinfo=UTC)
_T2 = datetime(2024, 1, 2, tzinfo=UTC)


def _d(s: Any = None, cfg: Any = None) -> MagicMock:
    o = MagicMock()
    o.session, o.base_url, o.config = s, _B, cfg
    o._metric_name = "tm"
    o._ensure_session = AsyncMock()
    return o


def _ok(d: dict[str, Any], st: int = HTTP_OK) -> AsyncMock:
    r = AsyncMock()
    r.status, r.json, r.text = (
        st,
        AsyncMock(return_value=d),
        AsyncMock(return_value="ok"),
    )
    r.__aenter__, r.__aexit__ = AsyncMock(return_value=r), AsyncMock(return_value=False)
    return r


def _er(st: int = HTTP_ERR) -> AsyncMock:
    r = AsyncMock()
    r.status, r.text = st, AsyncMock(return_value="e")
    r.__aenter__, r.__aexit__ = AsyncMock(return_value=r), AsyncMock(return_value=False)
    return r


def _vec(items: list[dict[str, Any]]) -> dict[str, Any]:
    return {"status": "success", "data": {"resultType": "vector", "result": items}}


def _vi(n: str = "up", lb: dict[str, str] | None = None) -> dict[str, Any]:
    m: dict[str, str] = {"__name__": n}
    if lb:
        m.update(lb)
    return {"metric": m, "value": [_TS, "1"]}


def _cx(st: int = HTTP_OK) -> MagicMock:
    r = MagicMock()
    r.status = st
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=r)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm


def _cfg() -> MagicMock:
    c = MagicMock()
    c.options, c.host = {}, "localhost"
    return c


def _am() -> AsyncMock:
    return AsyncMock()


class TestInstantQuery:
    async def test_ok(self) -> None:
        with patch(_RR, return_value=_ok(_vec([_vi()]))):
            r = await instant_query(_d(s=_am()), "up")
        assert len(r) == 1
        assert r[0]["metric_name"] == "up"

    async def test_time(self) -> None:
        with patch(_RR, return_value=_ok(_vec([]))) as m:
            await instant_query(_d(s=_am()), "up", time=_T1)
        assert "time" in m.call_args.kwargs["params"]

    async def test_no_sess(self) -> None:
        with pytest.raises(StorageError, match="not connected"):
            await instant_query(_d(), "up")

    async def test_err(self) -> None:
        with patch(_RR, return_value=_er(HTTP_BAD)), pytest.raises(StorageError):
            await instant_query(_d(s=_am()), "x")


class TestRangeQuery:
    async def test_ok(self) -> None:
        mx = {"metric": {"__name__": "c"}, "values": [[_TS, "1"], [_TS + 60, "2"]]}
        b = {"status": "success", "data": {"resultType": "matrix", "result": [mx]}}
        with patch(_RR, return_value=_ok(b)):
            assert len(await range_query(_d(s=_am()), "c", _T1, _T2)) == _TWO

    async def test_no_sess(self) -> None:
        with pytest.raises(StorageError, match="not connected"):
            await range_query(_d(), "c", _T1, _T2)

    async def test_err(self) -> None:
        with patch(_RR, return_value=_er()), pytest.raises(StorageError):
            await range_query(_d(s=_am()), "c", _T1, _T2)


class TestFindMetrics:
    async def test_ok(self) -> None:
        with patch(_RR, return_value=_ok(_vec([_vi(), _vi()]))):
            assert len(await find_metrics(_d(s=_am()), "h")) == _TWO

    async def test_limit_skip(self) -> None:
        b = _vec([_vi(), _vi()])
        with patch(_RR, return_value=_ok(b)):
            assert len(await find_metrics(_d(s=_am()), "m", limit=1)) == 1
        with patch(_RR, return_value=_ok(b)):
            assert len(await find_metrics(_d(s=_am()), "m", skip=1)) == 1


class TestFindMetricByLabels:
    async def test_found(self) -> None:
        with patch(_RR, return_value=_ok(_vec([_vi("up", {"j": "n"})]))):
            r = await find_metric_by_labels(_d(s=_am()), "up", {"j": "n"})
        assert r is not None

    async def test_empty(self) -> None:
        with patch(_RR, return_value=_ok(_vec([]))):
            assert await find_metric_by_labels(_d(s=_am()), "up", {"j": "x"}) is None


class TestGetSeries:
    async def test_ok(self) -> None:
        with patch(_RR, return_value=_ok({"data": [{"__name__": "up"}]})):
            assert len(await get_series(_d(s=_am()), ["up"])) == 1

    async def test_params(self) -> None:
        with patch(_RR, return_value=_ok({"data": []})) as m:
            await get_series(_d(s=_am()), ["up"], start=_T1, end=_T2)
        assert "start" in m.call_args.kwargs["params"]

    async def test_no_sess(self) -> None:
        with pytest.raises(StorageError):
            await get_series(_d(), ["up"])

    async def test_err(self) -> None:
        with patch(_RR, return_value=_er()), pytest.raises(StorageError):
            await get_series(_d(s=_am()), ["up"])


class TestLabelsAndValues:
    async def test_names_ok(self) -> None:
        with patch(_RR, return_value=_ok({"data": ["a", "b", "c"]})):
            assert len(await get_label_names(_d(s=_am()))) == _THREE

    async def test_names_match(self) -> None:
        with patch(_RR, return_value=_ok({"data": []})) as m:
            await get_label_names(_d(s=_am()), match=["up"], start=_T1, end=_T2)
        assert "match[]" in m.call_args.kwargs["params"]

    async def test_names_no_sess(self) -> None:
        with pytest.raises(StorageError):
            await get_label_names(_d())

    async def test_names_err(self) -> None:
        with patch(_RR, return_value=_er()), pytest.raises(StorageError):
            await get_label_names(_d(s=_am()))

    async def test_vals_ok(self) -> None:
        with patch(_RR, return_value=_ok({"data": ["a", "b"]})):
            assert len(await get_label_values(_d(s=_am()), "j")) == _TWO

    async def test_vals_no_sess(self) -> None:
        with pytest.raises(StorageError):
            await get_label_values(_d(), "j")

    async def test_vals_err(self) -> None:
        with patch(_RR, return_value=_er()), pytest.raises(StorageError):
            await get_label_values(_d(s=_am()), "j")


class TestMetadataRead:
    async def test_ok(self) -> None:
        with patch(_RR, return_value=_ok({"data": {"up": []}})):
            assert "up" in await get_metric_metadata(_d(s=_am()), "up")

    async def test_no_name(self) -> None:
        with patch(_RR, return_value=_ok({"data": {}})) as m:
            await get_metric_metadata(_d(s=_am()))
        assert "metric" not in m.call_args.kwargs.get("params", {})

    async def test_no_sess(self) -> None:
        with pytest.raises(StorageError):
            await get_metric_metadata(_d())

    async def test_err(self) -> None:
        with patch(_RR, return_value=_er()), pytest.raises(StorageError):
            await get_metric_metadata(_d(s=_am()))


class TestTargetsRulesAlerts:
    async def test_targets(self) -> None:
        with patch(_RR, return_value=_ok({"data": {"at": []}})):
            assert "at" in await get_targets(_d(s=_am()))

    async def test_targets_errors(self) -> None:
        with pytest.raises(StorageError):
            await get_targets(_d())
        with patch(_RR, return_value=_er()), pytest.raises(StorageError):
            await get_targets(_d(s=_am()))

    async def test_rules(self) -> None:
        with patch(_RR, return_value=_ok({"data": {"groups": []}})):
            assert "groups" in await get_rules(_d(s=_am()))

    async def test_rules_errors(self) -> None:
        with pytest.raises(StorageError):
            await get_rules(_d())
        with patch(_RR, return_value=_er()), pytest.raises(StorageError):
            await get_rules(_d(s=_am()))

    async def test_alerts(self) -> None:
        with patch(_RR, return_value=_ok({"data": {"alerts": [{}]}})):
            assert len(await get_alerts(_d(s=_am()))) == 1
        with patch(_RR, return_value=_ok({"data": {"alerts": []}})):
            assert await get_alerts(_d(s=_am())) == []

    async def test_alerts_no_sess(self) -> None:
        with pytest.raises(StorageError):
            await get_alerts(_d())


class TestWriteOps:
    async def test_write_empty(self) -> None:
        assert await write_metrics(_d(s=_am()), []) == 0

    async def test_write_ok(self) -> None:
        ms = [{"metric_name": "up", "labels": {}, "value": 1}]
        with patch(_RW, return_value=_ok({})):
            assert await write_metrics(_d(s=_am()), ms) == 1

    async def test_write_no_sess(self) -> None:
        with pytest.raises(StorageError):
            await write_metrics(_d(), [{"metric_name": "x", "value": 1}])

    async def test_single_ok(self) -> None:
        with patch(_RW, return_value=_ok({})):
            assert (
                await write_single_metric(_d(s=_am()), "c", 0.5, labels={"h": "s"})
                == "c{h=s}"
            )
            assert await write_single_metric(_d(s=_am()), "up", 1.0) == "up{}"

    async def test_single_fail(self) -> None:
        with (
            patch(_RW, side_effect=StorageError("x")),
            patch(_GW, new_callable=AsyncMock, return_value=0),
            pytest.raises(StorageError, match="Failed"),
        ):
            await write_single_metric(_d(s=_am()), "b", 0.0)


class TestDeleteCleanSnap:
    async def test_delete_ok_and_time(self) -> None:
        with patch(_RW, return_value=_ok({}, HTTP_NO_CONTENT)):
            assert await delete_series(_d(s=_am()), ["up"]) == 1
        with patch(_RW, return_value=_ok({}, HTTP_NO_CONTENT)):
            assert (
                await delete_series(_d(s=_am()), ["a", "b"], start=_T1, end=_T2) == _TWO
            )

    async def test_delete_errors(self) -> None:
        with pytest.raises(StorageError):
            await delete_series(_d(), ["up"])
        with patch(_RW, return_value=_er()), pytest.raises(StorageError):
            await delete_series(_d(s=_am()), ["up"])

    async def test_clean(self) -> None:
        with patch(_RW, return_value=_ok({}, HTTP_NO_CONTENT)):
            await clean_tombstones(_d(s=_am()))
        with pytest.raises(StorageError):
            await clean_tombstones(_d())
        with patch(_RW, return_value=_er()), pytest.raises(StorageError):
            await clean_tombstones(_d(s=_am()))

    async def test_snap_ok_and_head(self) -> None:
        with patch(_RW, return_value=_ok({"data": {"name": "s1"}})):
            assert await snapshot(_d(s=_am())) == "s1"
        with patch(_RW, return_value=_ok({"data": {"name": "s"}})) as m:
            await snapshot(_d(s=_am()), skip_head=True)
        assert m.call_args.kwargs["params"]["skip_head"] == "true"

    async def test_snap_errors(self) -> None:
        with pytest.raises(StorageError):
            await snapshot(_d())
        with patch(_RW, return_value=_er()), pytest.raises(StorageError):
            await snapshot(_d(s=_am()))


class TestGatewayDelete:
    async def test_ok_and_grouping(self) -> None:
        with patch(_RW, return_value=_ok({}, HTTP_NO_CONTENT)):
            assert await delete_from_gateway(_d(s=_am(), cfg=_cfg())) is True
        with patch(_RW, return_value=_ok({}, HTTP_ACCEPTED)):
            assert (
                await delete_from_gateway(
                    _d(s=_am(), cfg=_cfg()), job_name="j", grouping={"e": "p"}
                )
                is True
            )

    async def test_fail(self) -> None:
        with patch(_RW, return_value=_er(HTTP_BAD)):
            assert await delete_from_gateway(_d(s=_am(), cfg=_cfg())) is False

    async def test_no_sess(self) -> None:
        with pytest.raises(StorageError):
            await delete_from_gateway(_d())


class TestConnection:
    async def test_create(self) -> None:
        s = await create_session()
        assert s is not None
        await s.close()

    async def test_create_token(self) -> None:
        c = MagicMock()
        c.options = {"auth_token": "t", "timeout": "5"}
        s = await create_session(c)
        assert dict(s._default_headers)["Authorization"] == "Bearer t"
        await s.close()

    async def test_create_key(self) -> None:
        c = MagicMock()
        c.options = {"api_key": "k"}
        s = await create_session(c)
        assert dict(s._default_headers)["X-API-Key"] == "k"
        await s.close()

    async def test_close(self) -> None:
        s = AsyncMock()
        s.closed = False
        await close_session(s)
        s.close.assert_awaited_once()
        s2 = AsyncMock()
        s2.closed = True
        await close_session(s2)
        s2.close.assert_not_awaited()
        await close_session(None)

    async def _probe(self, fn: Any, st: int, expect: bool) -> None:
        s = MagicMock()
        s.get = MagicMock(return_value=_cx(st))
        assert await fn(s, _B) is expect

    async def test_healthy(self) -> None:
        await self._probe(health_check, HTTP_OK, True)

    async def test_unhealthy(self) -> None:
        await self._probe(health_check, HTTP_ERR, False)

    async def test_health_timeout(self) -> None:
        s = MagicMock()
        s.get = MagicMock(side_effect=TimeoutError())
        assert await health_check(s, _B) is False

    async def test_ready(self) -> None:
        await self._probe(check_ready, HTTP_OK, True)

    async def test_not_ready(self) -> None:
        await self._probe(check_ready, HTTP_ERR, False)

    async def test_ready_err(self) -> None:
        s = MagicMock()
        s.get = MagicMock(side_effect=TimeoutError())
        assert await check_ready(s, _B) is False


def _mp(target: str, **kw: Any) -> Any:
    return patch(f"{_M}.{target}", new_callable=AsyncMock, **kw)


class TestMetadataOps:
    async def test_list_db(self) -> None:
        c = MagicMock()
        c.database = "mydb"
        assert await list_databases(_d(cfg=c)) == ["mydb"]
        assert await list_databases(_d(cfg=None)) == [_B]

    async def test_schemas(self) -> None:
        with _mp("get_label_names", return_value=["a"]):
            assert await list_schemas(_d(s=_am())) == ["a"]
        with _mp("get_label_names", side_effect=StorageError("x")):
            assert await list_schemas(_d(s=_am())) == ["__name__"]

    async def test_models(self) -> None:
        with _mp("get_label_values", return_value=["a", "b"]):
            assert len(await list_models(_d(s=_am()))) == _TWO
        with _mp("get_label_values", side_effect=StorageError("x")):
            assert await list_models(_d(s=_am())) == ["tm"]

    async def test_info_found(self) -> None:
        v = {"up": [{"type": "gauge", "help": "h", "unit": ""}]}
        with _mp("get_metric_metadata", return_value=v):
            assert (await get_model_info(_d(s=_am()), "up"))["type"] == "gauge"

    async def test_info_missing_and_err(self) -> None:
        with _mp("get_metric_metadata", return_value={}):
            assert (await get_model_info(_d(s=_am()), "x"))["type"] == "unknown"
        with _mp("get_metric_metadata", side_effect=StorageError("x")):
            assert (await get_model_info(_d(s=_am()), "e"))["type"] == "unknown"

    async def test_schema_ok(self) -> None:
        with (
            _mp("get_model_info", return_value={"type": "g", "help": ""}),
            _mp("get_label_names", return_value=["__name__", "j"]),
        ):
            assert "j" in (await get_model_schema(_d(s=_am()), "up"))["fields"]

    async def test_schema_label_err(self) -> None:
        with (
            _mp("get_model_info", return_value={"type": "c", "help": ""}),
            _mp("get_label_names", side_effect=StorageError("x")),
        ):
            r = await get_model_schema(_d(s=_am()), "m")
            assert "__name__" in r["fields"]

    async def test_fields(self) -> None:
        fs = {
            "__name__": {"type": "string", "required": True},
            "v": {"type": "float", "required": True},
        }
        with _mp("get_model_schema", return_value={"fields": fs}):
            assert len(await get_model_fields(_d(s=_am()), "up")) == _TWO

    async def test_indexes_ok(self) -> None:
        with _mp("get_label_names", return_value=["__name__", "j", "i"]):
            r = await get_model_indexes(_d(s=_am()), "up")
        assert len(r) == _TWO
        assert r[0]["field"] == "j"

    async def test_indexes_err(self) -> None:
        with _mp("get_label_names", side_effect=StorageError("x")):
            assert await get_model_indexes(_d(s=_am()), "up") == []

    async def test_conn_healthy(self) -> None:
        with _mp("health_check", return_value=True):
            assert await prom_test_connection(_d(s=_am())) is True

    async def test_conn_errors(self) -> None:
        with _mp("health_check", return_value=False):
            assert await prom_test_connection(_d(s=_am())) is False
        assert await prom_test_connection(_d()) is False
        with _mp("health_check", side_effect=RuntimeError("x")):
            assert await prom_test_connection(_d(s=_am())) is False
