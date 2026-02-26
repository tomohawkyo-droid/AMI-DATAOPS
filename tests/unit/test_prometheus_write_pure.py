"""Tests for pure helpers in prometheus_write."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from ami.implementations.timeseries.prometheus_write import (
    _format_exposition_lines,
    _get_pushgateway_url,
)

_EXPECTED_TWO = 2


class _FakeDAO:
    def __init__(self, config=None):
        self.config = config


# ---------------------------------------------------------------
# _get_pushgateway_url
# ---------------------------------------------------------------


class TestGetPushgatewayUrlFromOptions:
    """URL derived from the config options dict."""

    def test_returns_explicit_url(self) -> None:
        config = MagicMock()
        config.options = {"pushgateway_url": "http://gw.local:9091"}
        dao = _FakeDAO(config=config)

        assert _get_pushgateway_url(dao) == "http://gw.local:9091"

    def test_strips_trailing_slash(self) -> None:
        config = MagicMock()
        config.options = {"pushgateway_url": "http://gw.local:9091/"}
        dao = _FakeDAO(config=config)

        assert _get_pushgateway_url(dao) == "http://gw.local:9091"

    def test_strips_multiple_trailing_slashes(self) -> None:
        config = MagicMock()
        config.options = {"pushgateway_url": "http://gw.local:9091///"}
        dao = _FakeDAO(config=config)

        assert _get_pushgateway_url(dao) == "http://gw.local:9091"


class TestGetPushgatewayUrlDefault:
    """URL derived from host/port when no explicit URL is set."""

    def test_uses_config_host(self) -> None:
        config = MagicMock()
        config.host = "prom.example.com"
        config.options = {}
        dao = _FakeDAO(config=config)

        result = _get_pushgateway_url(dao)
        assert result == "http://prom.example.com:9091"

    def test_defaults_to_localhost_when_no_config(self) -> None:
        dao = _FakeDAO(config=None)

        result = _get_pushgateway_url(dao)
        assert result == "http://localhost:9091"

    def test_defaults_to_localhost_when_host_is_none(self) -> None:
        config = MagicMock()
        config.host = None
        config.options = {}
        dao = _FakeDAO(config=config)

        result = _get_pushgateway_url(dao)
        assert result == "http://localhost:9091"

    def test_options_none_uses_host(self) -> None:
        config = MagicMock()
        config.host = "metrics.internal"
        config.options = None
        dao = _FakeDAO(config=config)

        result = _get_pushgateway_url(dao)
        assert result == "http://metrics.internal:9091"


# ---------------------------------------------------------------
# _format_exposition_lines
# ---------------------------------------------------------------


class TestFormatExpositionLinesSingle:
    """Formatting a single metric dict."""

    def test_metric_without_labels(self) -> None:
        metrics = [
            {
                "metric_name": "up",
                "labels": {},
                "value": 1,
            }
        ]
        lines = _format_exposition_lines(metrics)
        assert lines == ["up 1"]

    def test_metric_with_single_label(self) -> None:
        metrics = [
            {
                "metric_name": "http_requests_total",
                "labels": {"method": "GET"},
                "value": 42,
            }
        ]
        lines = _format_exposition_lines(metrics)
        expected = 'http_requests_total{method="GET"} 42'
        assert lines == [expected]

    def test_metric_with_multiple_labels_sorted(self) -> None:
        metrics = [
            {
                "metric_name": "http_requests_total",
                "labels": {"status": "200", "method": "POST"},
                "value": 7,
            }
        ]
        lines = _format_exposition_lines(metrics)
        expected = 'http_requests_total{method="POST",status="200"} 7'
        assert lines == [expected]

    def test_empty_labels_omits_braces(self) -> None:
        metrics = [
            {
                "metric_name": "node_load1",
                "labels": {},
                "value": 0.45,
            }
        ]
        lines = _format_exposition_lines(metrics)
        assert lines == ["node_load1 0.45"]

    def test_missing_labels_key_treated_as_empty(self) -> None:
        metrics = [{"metric_name": "go_goroutines", "value": 35}]
        lines = _format_exposition_lines(metrics)
        assert lines == ["go_goroutines 35"]


class TestFormatExpositionLinesTimestamp:
    """Timestamp handling in exposition output."""

    def test_datetime_appended_as_milliseconds(self) -> None:
        ts = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        ts_ms = int(ts.timestamp() * 1000)
        metrics = [
            {
                "metric_name": "cpu_temp",
                "labels": {},
                "value": 72.5,
                "timestamp": ts,
            }
        ]
        lines = _format_exposition_lines(metrics)
        assert lines == [f"cpu_temp 72.5 {ts_ms}"]

    def test_int_timestamp_appended_as_is(self) -> None:
        metrics = [
            {
                "metric_name": "cpu_temp",
                "labels": {},
                "value": 72.5,
                "timestamp": 1718452800000,
            }
        ]
        lines = _format_exposition_lines(metrics)
        assert lines == ["cpu_temp 72.5 1718452800000"]

    def test_float_timestamp_appended_as_is(self) -> None:
        metrics = [
            {
                "metric_name": "cpu_temp",
                "labels": {},
                "value": 72.5,
                "timestamp": 1718452800.123,
            }
        ]
        lines = _format_exposition_lines(metrics)
        assert lines == ["cpu_temp 72.5 1718452800.123"]

    def test_none_timestamp_omitted(self) -> None:
        metrics = [
            {
                "metric_name": "up",
                "labels": {},
                "value": 1,
                "timestamp": None,
            }
        ]
        lines = _format_exposition_lines(metrics)
        assert lines == ["up 1"]


class TestFormatExpositionLinesMultiple:
    """Formatting lists with more than one metric."""

    def test_two_metrics_produce_two_lines(self) -> None:
        metrics = [
            {"metric_name": "up", "labels": {}, "value": 1},
            {
                "metric_name": "node_load1",
                "labels": {},
                "value": 0.3,
            },
        ]
        lines = _format_exposition_lines(metrics)
        assert len(lines) == _EXPECTED_TWO
        assert lines[0] == "up 1"
        assert lines[1] == "node_load1 0.3"

    def test_empty_list_returns_empty(self) -> None:
        assert _format_exposition_lines([]) == []

    def test_defaults_for_missing_keys(self) -> None:
        metrics = [{}]
        lines = _format_exposition_lines(metrics)
        assert lines == ["unknown 0"]
