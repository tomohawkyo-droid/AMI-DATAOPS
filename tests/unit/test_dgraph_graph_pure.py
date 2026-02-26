"""Tests for pure functions in dgraph_graph module."""

from __future__ import annotations

import pytest

from ami.implementations.graph.dgraph_graph import (
    _count_degrees,
    _format_degree_result,
    _validate_direction,
    _validate_identifier,
    _validate_positive_int,
    _validate_uid,
)

EXPECT_TEN = 10
EXPECT_TWO = 2
EXPECT_THREE = 3
EXPECT_FIVE = 5
EXPECT_EIGHT = 8


class TestValidateUid:
    """Validate UID format checking."""

    def test_valid_hex_uid(self) -> None:
        assert _validate_uid("0x1abc") == "0x1abc"

    def test_valid_hex_uid_uppercase(self) -> None:
        assert _validate_uid("0xABCDEF") == "0xABCDEF"

    def test_valid_alphanumeric_uid(self) -> None:
        assert _validate_uid("abc123") == "abc123"

    def test_valid_uid_with_hyphen(self) -> None:
        assert _validate_uid("my-uid") == "my-uid"

    def test_valid_uid_with_underscore(self) -> None:
        assert _validate_uid("uid_1") == "uid_1"

    def test_empty_uid_raises(self) -> None:
        with pytest.raises(ValueError, match="cannot be empty"):
            _validate_uid("")

    def test_semicolon_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid UID"):
            _validate_uid("uid;drop")

    def test_braces_raise(self) -> None:
        with pytest.raises(ValueError, match="Invalid UID"):
            _validate_uid("uid{}")

    def test_angle_brackets_raise(self) -> None:
        with pytest.raises(ValueError, match="Invalid UID"):
            _validate_uid("uid<script>")

    def test_space_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid UID"):
            _validate_uid("uid name")


class TestValidateIdentifier:
    """Validate identifier format checking."""

    def test_valid_type_name(self) -> None:
        assert _validate_identifier("MyType") == "MyType"

    def test_valid_underscore_name(self) -> None:
        result = _validate_identifier("test_type")
        assert result == "test_type"

    def test_valid_dotted_name(self) -> None:
        result = _validate_identifier("type.name")
        assert result == "type.name"

    def test_valid_hyphenated_name(self) -> None:
        result = _validate_identifier("type-name")
        assert result == "type-name"

    def test_empty_raises(self) -> None:
        with pytest.raises(ValueError, match="cannot be empty"):
            _validate_identifier("")

    def test_space_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid identifier"):
            _validate_identifier("type name")

    def test_semicolon_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid identifier"):
            _validate_identifier("type;drop")

    def test_braces_raise(self) -> None:
        with pytest.raises(ValueError, match="Invalid identifier"):
            _validate_identifier("type{}")


class TestValidatePositiveInt:
    """Validate positive integer checking."""

    def test_one_is_valid(self) -> None:
        assert _validate_positive_int(1) == 1

    def test_ten_is_valid(self) -> None:
        result = _validate_positive_int(EXPECT_TEN)
        assert result == EXPECT_TEN

    def test_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="positive integer"):
            _validate_positive_int(0)

    def test_negative_raises(self) -> None:
        with pytest.raises(ValueError, match="positive integer"):
            _validate_positive_int(-3)

    def test_string_raises(self) -> None:
        with pytest.raises(ValueError, match="positive integer"):
            _validate_positive_int("5")


class TestValidateDirection:
    """Validate direction parameter checking."""

    @pytest.mark.parametrize("direction", ["in", "out", "all"])
    def test_valid_directions(self, direction: str) -> None:
        _validate_direction(direction)

    def test_both_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid direction"):
            _validate_direction("both")

    def test_up_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid direction"):
            _validate_direction("up")

    def test_empty_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid direction"):
            _validate_direction("")


class TestCountDegrees:
    """Validate in/out degree counting from node data."""

    def test_out_edges_only(self) -> None:
        node = {
            "uid": "0x1",
            "friends": [{"uid": "0x2"}],
        }
        in_deg, out_deg = _count_degrees(node)
        assert in_deg == 0
        assert out_deg == 1

    def test_reverse_edges_only(self) -> None:
        node = {
            "uid": "0x1",
            "~friends": [
                {"uid": "0x2"},
                {"uid": "0x3"},
            ],
        }
        in_deg, out_deg = _count_degrees(node)
        assert in_deg == EXPECT_TWO
        assert out_deg == 0

    def test_empty_node(self) -> None:
        node = {
            "uid": "0x1",
            "dgraph.type": ["Person"],
        }
        in_deg, out_deg = _count_degrees(node)
        assert in_deg == 0
        assert out_deg == 0

    def test_mixed_edges(self) -> None:
        node = {
            "uid": "0x1",
            "dgraph.type": ["Person"],
            "friends": [
                {"uid": "0x2"},
                {"uid": "0x3"},
            ],
            "works_at": [{"uid": "0x10"}],
            "~follows": [{"uid": "0x4"}],
        }
        in_deg, out_deg = _count_degrees(node)
        assert in_deg == 1
        assert out_deg == EXPECT_THREE

    def test_scalar_values_ignored(self) -> None:
        node = {
            "uid": "0x1",
            "name": "Alice",
            "age": 30,
            "friends": [{"uid": "0x2"}],
        }
        in_deg, out_deg = _count_degrees(node)
        assert in_deg == 0
        assert out_deg == 1


class TestFormatDegreeResult:
    """Validate degree result formatting by direction."""

    def test_direction_in(self) -> None:
        result = _format_degree_result("in", EXPECT_THREE, EXPECT_FIVE)
        assert result == {"in": EXPECT_THREE}

    def test_direction_out(self) -> None:
        result = _format_degree_result("out", EXPECT_THREE, EXPECT_FIVE)
        assert result == {"out": EXPECT_FIVE}

    def test_direction_all(self) -> None:
        result = _format_degree_result("all", EXPECT_THREE, EXPECT_FIVE)
        assert result == {
            "in": EXPECT_THREE,
            "out": EXPECT_FIVE,
            "total": EXPECT_EIGHT,
        }

    def test_direction_all_zeros(self) -> None:
        result = _format_degree_result("all", 0, 0)
        assert result == {
            "in": 0,
            "out": 0,
            "total": 0,
        }
