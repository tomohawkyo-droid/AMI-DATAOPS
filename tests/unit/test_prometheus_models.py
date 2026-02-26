"""Tests for prometheus_models pure functions."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ami.core.exceptions import StorageValidationError
from ami.implementations.timeseries.prometheus_models import (
    PrometheusMetric,
    PromQLBuilder,
    _build_dict_selector,
    _escape_promql_value,
    _validate_promql_label,
    _validate_regex_pattern,
    dict_query_to_promql,
    parse_prometheus_response,
)

# Epoch for 2024-01-01T00:00:00Z used across response tests
_EPOCH_2024 = 1704067200.0
_EPOCH_2024_PLUS_60 = 1704067260.0


class TestEscapePromqlValue:
    def test_backslash(self) -> None:
        assert _escape_promql_value("a\\b") == "a\\\\b"

    def test_double_quote(self) -> None:
        assert _escape_promql_value('a"b') == 'a\\"b'

    def test_newline(self) -> None:
        assert _escape_promql_value("a\nb") == "a\\nb"

    def test_clean_string(self) -> None:
        assert _escape_promql_value("hello") == "hello"

    def test_combined(self) -> None:
        result = _escape_promql_value('a\\b"c\nd')
        assert result == 'a\\\\b\\"c\\nd'

    def test_empty_string(self) -> None:
        assert _escape_promql_value("") == ""

    def test_multiple_newlines(self) -> None:
        assert _escape_promql_value("\n\n") == "\\n\\n"


class TestValidatePromqlLabel:
    def test_valid_simple(self) -> None:
        _validate_promql_label("method")

    def test_valid_with_underscore_prefix(self) -> None:
        _validate_promql_label("_private")

    def test_valid_with_digits(self) -> None:
        _validate_promql_label("code_2xx")

    def test_invalid_starts_with_digit(self) -> None:
        with pytest.raises(StorageValidationError):
            _validate_promql_label("2xx")

    def test_invalid_contains_dash(self) -> None:
        with pytest.raises(StorageValidationError):
            _validate_promql_label("my-label")

    def test_invalid_contains_dot(self) -> None:
        with pytest.raises(StorageValidationError):
            _validate_promql_label("my.label")

    def test_invalid_empty(self) -> None:
        with pytest.raises(StorageValidationError):
            _validate_promql_label("")

    def test_error_message_includes_key(self) -> None:
        with pytest.raises(StorageValidationError, match="Invalid PromQL"):
            _validate_promql_label("bad-key")


class TestPrometheusMetric:
    def test_selector_no_labels(self) -> None:
        m = PrometheusMetric(metric_name="http_requests_total")
        assert m.to_promql_selector() == "http_requests_total"

    def test_selector_single_label(self) -> None:
        m = PrometheusMetric(
            metric_name="http_requests_total",
            labels={"method": "GET"},
        )
        assert m.to_promql_selector() == 'http_requests_total{method="GET"}'

    def test_selector_multiple_labels_sorted(self) -> None:
        m = PrometheusMetric(
            metric_name="http_requests_total",
            labels={"status": "200", "method": "POST"},
        )
        result = m.to_promql_selector()
        expected = 'http_requests_total{method="POST",status="200"}'
        assert result == expected

    def test_selector_special_chars_escaped(self) -> None:
        m = PrometheusMetric(
            metric_name="http_requests_total",
            labels={"path": '/api/"v1'},
        )
        result = m.to_promql_selector()
        assert result == 'http_requests_total{path="/api/\\"v1"}'

    def test_exposition_line_no_timestamp(self) -> None:
        m = PrometheusMetric(
            metric_name="http_requests_total",
            value=42.0,
        )
        assert m.to_exposition_line() == "http_requests_total 42.0"

    def test_exposition_line_with_timestamp(self) -> None:
        ts = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        m = PrometheusMetric(
            metric_name="http_requests_total",
            value=100.0,
            timestamp=ts,
        )
        ts_ms = int(ts.timestamp() * 1000)
        expected = f"http_requests_total 100.0 {ts_ms}"
        assert m.to_exposition_line() == expected

    def test_exposition_line_with_labels(self) -> None:
        m = PrometheusMetric(
            metric_name="http_requests_total",
            labels={"method": "GET"},
            value=7.0,
        )
        expected = 'http_requests_total{method="GET"} 7.0'
        assert m.to_exposition_line() == expected

    def test_exposition_with_labels_and_ts(self) -> None:
        ts = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        m = PrometheusMetric(
            metric_name="node_cpu_seconds_total",
            labels={"cpu": "0", "mode": "idle"},
            value=12345.6,
            timestamp=ts,
        )
        ts_ms = int(ts.timestamp() * 1000)
        expected = f'node_cpu_seconds_total{{cpu="0",mode="idle"}} 12345.6 {ts_ms}'
        assert m.to_exposition_line() == expected

    def test_default_value_is_zero(self) -> None:
        m = PrometheusMetric(metric_name="http_requests_total")
        assert m.value == 0.0


class TestPromQLBuilder:
    def test_instant_query_no_labels(self) -> None:
        result = PromQLBuilder.instant_query("http_requests_total")
        assert result == "http_requests_total"

    def test_instant_query_with_labels(self) -> None:
        result = PromQLBuilder.instant_query(
            "http_requests_total",
            {"method": "GET"},
        )
        expected = 'http_requests_total{method="GET"}'
        assert result == expected

    def test_instant_query_empty_labels(self) -> None:
        result = PromQLBuilder.instant_query("up", {})
        assert result == "up"

    def test_range_query_default_duration(self) -> None:
        result = PromQLBuilder.range_query("http_requests_total")
        assert result == "http_requests_total[5m]"

    def test_range_query_custom_duration(self) -> None:
        result = PromQLBuilder.range_query(
            "http_requests_total",
            {"method": "GET"},
            "1h",
        )
        expected = 'http_requests_total{method="GET"}[1h]'
        assert result == expected

    def test_rate_query_defaults(self) -> None:
        result = PromQLBuilder.rate_query("http_requests_total")
        assert result == "rate(http_requests_total[5m])"

    def test_rate_query_with_labels(self) -> None:
        result = PromQLBuilder.rate_query(
            "http_requests_total",
            {"status": "500"},
            "10m",
        )
        expected = 'rate(http_requests_total{status="500"}[10m])'
        assert result == expected

    def test_aggregation_without_by(self) -> None:
        result = PromQLBuilder.aggregation_query("sum", "http_requests_total")
        assert result == "sum(http_requests_total)"

    def test_aggregation_with_by(self) -> None:
        result = PromQLBuilder.aggregation_query(
            "sum",
            "http_requests_total",
            by=["method"],
        )
        expected = "sum by (method) (http_requests_total)"
        assert result == expected

    def test_aggregation_with_labels_and_by(self) -> None:
        result = PromQLBuilder.aggregation_query(
            "avg",
            "cpu_usage",
            {"instance": "node1"},
            by=["cpu", "mode"],
        )
        expected = 'avg by (cpu, mode) (cpu_usage{instance="node1"})'
        assert result == expected


class TestValidateRegexPattern:
    def test_valid_pattern(self) -> None:
        _validate_regex_pattern("/api.*")

    def test_curly_brace_open(self) -> None:
        with pytest.raises(ValueError, match="Invalid regex"):
            _validate_regex_pattern("test{bad")

    def test_curly_brace_close(self) -> None:
        with pytest.raises(ValueError, match="Invalid regex"):
            _validate_regex_pattern("test}bad")

    def test_both_braces(self) -> None:
        with pytest.raises(ValueError, match="Invalid regex"):
            _validate_regex_pattern("{}")

    def test_clean_regex(self) -> None:
        _validate_regex_pattern("^(GET|POST)$")


class TestBuildDictSelector:
    def test_ne_operator(self) -> None:
        result = _build_dict_selector("method", {"$ne": "POST"})
        assert result == 'method!="POST"'

    def test_regex_operator(self) -> None:
        result = _build_dict_selector("path", {"$regex": "/api.*"})
        assert result == 'path=~"/api.*"'

    def test_nregex_operator(self) -> None:
        result = _build_dict_selector("path", {"$nregex": "/health"})
        assert result == 'path!~"/health"'

    def test_unsupported_operator_returns_none(
        self,
    ) -> None:
        result = _build_dict_selector("method", {"$gt": 5})
        assert result is None

    def test_invalid_label_raises(self) -> None:
        with pytest.raises(StorageValidationError):
            _build_dict_selector("bad-key", {"$ne": "x"})

    def test_ne_escapes_value(self) -> None:
        result = _build_dict_selector("method", {"$ne": 'PO"ST'})
        assert result == 'method!="PO\\"ST"'

    def test_regex_with_bad_pattern(self) -> None:
        with pytest.raises(ValueError, match="Invalid regex"):
            _build_dict_selector("path", {"$regex": "bad{pattern"})

    def test_nregex_with_bad_pattern(self) -> None:
        with pytest.raises(ValueError, match="Invalid regex"):
            _build_dict_selector("path", {"$nregex": "bad}pattern"})


class TestDictQueryToPromql:
    def test_empty_query(self) -> None:
        result = dict_query_to_promql("http_requests_total", {})
        assert result == "http_requests_total"

    def test_equality(self) -> None:
        result = dict_query_to_promql("http_requests_total", {"method": "GET"})
        expected = 'http_requests_total{method="GET"}'
        assert result == expected

    def test_ne_operator(self) -> None:
        result = dict_query_to_promql(
            "http_requests_total",
            {"method": {"$ne": "DELETE"}},
        )
        expected = 'http_requests_total{method!="DELETE"}'
        assert result == expected

    def test_regex_operator(self) -> None:
        result = dict_query_to_promql(
            "http_requests_total",
            {"path": {"$regex": "/api.*"}},
        )
        expected = 'http_requests_total{path=~"/api.*"}'
        assert result == expected

    def test_dollar_prefixed_keys_skipped(self) -> None:
        result = dict_query_to_promql(
            "http_requests_total",
            {"$limit": 10, "method": "GET"},
        )
        expected = 'http_requests_total{method="GET"}'
        assert result == expected

    def test_only_dollar_keys_returns_metric(
        self,
    ) -> None:
        result = dict_query_to_promql(
            "http_requests_total",
            {"$limit": 10, "$sort": "desc"},
        )
        assert result == "http_requests_total"

    def test_mixed_equality_and_dict(self) -> None:
        result = dict_query_to_promql(
            "http_requests_total",
            {
                "method": "GET",
                "status": {"$ne": "500"},
            },
        )
        assert "method=" in result
        assert "status!=" in result

    def test_unsupported_dict_op_excluded(self) -> None:
        result = dict_query_to_promql(
            "http_requests_total",
            {"code": {"$gt": 400}},
        )
        assert result == "http_requests_total"


class TestParsePrometheusResponse:
    def test_vector_single_result(self) -> None:
        response = {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {
                        "metric": {
                            "__name__": "up",
                            "instance": "localhost:9090",
                            "job": "prometheus",
                        },
                        "value": [
                            _EPOCH_2024,
                            "1",
                        ],
                    }
                ],
            },
        }
        records = parse_prometheus_response(response)
        assert len(records) == 1
        rec = records[0]
        assert rec["metric_name"] == "up"
        assert rec["labels"] == {
            "instance": "localhost:9090",
            "job": "prometheus",
        }
        assert rec["value"] == 1.0
        assert rec["timestamp"] == datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)

    def test_vector_empty_value(self) -> None:
        response = {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {
                        "metric": {
                            "__name__": "up",
                        },
                        "value": [],
                    }
                ],
            },
        }
        records = parse_prometheus_response(response)
        assert len(records) == 1
        assert "timestamp" not in records[0]
        assert "value" not in records[0]

    def test_matrix_type(self) -> None:
        response = {
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [
                    {
                        "metric": {
                            "__name__": "up",
                            "job": "node",
                        },
                        "values": [
                            [_EPOCH_2024, "1"],
                            [_EPOCH_2024_PLUS_60, "0"],
                        ],
                    }
                ],
            },
        }
        records = parse_prometheus_response(response)
        expected_count = 2
        assert len(records) == expected_count
        assert records[0]["metric_name"] == "up"
        assert records[0]["labels"] == {"job": "node"}
        assert records[0]["value"] == 1.0
        assert records[1]["value"] == 0.0

    def test_non_success_status(self) -> None:
        response = {
            "status": "error",
            "errorType": "bad_data",
            "error": "invalid query",
        }
        records = parse_prometheus_response(response)
        assert records == []

    def test_unknown_result_type(self) -> None:
        response = {
            "status": "success",
            "data": {
                "resultType": "scalar",
                "result": [_EPOCH_2024, "1"],
            },
        }
        records = parse_prometheus_response(response)
        assert records == []

    def test_vector_multiple_results(self) -> None:
        response = {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {
                        "metric": {
                            "__name__": "up",
                            "job": "a",
                        },
                        "value": [_EPOCH_2024, "1"],
                    },
                    {
                        "metric": {
                            "__name__": "up",
                            "job": "b",
                        },
                        "value": [_EPOCH_2024, "0"],
                    },
                ],
            },
        }
        records = parse_prometheus_response(response)
        expected_count = 2
        assert len(records) == expected_count
        assert records[0]["labels"]["job"] == "a"
        assert records[1]["labels"]["job"] == "b"

    def test_matrix_short_value_skipped(self) -> None:
        response = {
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [
                    {
                        "metric": {
                            "__name__": "up",
                        },
                        "values": [
                            [_EPOCH_2024],
                            [_EPOCH_2024_PLUS_60, "1"],
                        ],
                    }
                ],
            },
        }
        records = parse_prometheus_response(response)
        assert len(records) == 1
        assert records[0]["value"] == 1.0

    def test_missing_data_key(self) -> None:
        response = {"status": "success"}
        records = parse_prometheus_response(response)
        assert records == []

    def test_missing_status_key(self) -> None:
        response = {"data": {"resultType": "vector"}}
        records = parse_prometheus_response(response)
        assert records == []
