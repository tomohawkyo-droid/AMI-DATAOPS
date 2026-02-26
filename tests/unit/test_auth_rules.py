"""Tests for AuthRule.to_dgraph_rule().

Covers Issue #69 -- verify jwt_rule, graph_traversal_rule, and unknown
type raises ValueError (per Issue #43 fix).
"""

import pytest

from ami.models.security import AuthRule


class TestToDgraphRule:
    """Verify AuthRule.to_dgraph_rule() for all rule types."""

    def test_jwt_rule_returns_query(self) -> None:
        rule = AuthRule(
            name="jwt_auth",
            rule_type="jwt",
            rule_config={"query": "{ queryUser(filter: { id: [$jwt.sub] }) }"},
        )
        result = rule.to_dgraph_rule()
        assert result == "{ queryUser(filter: { id: [$jwt.sub] }) }"

    def test_jwt_rule_missing_query_returns_empty(self) -> None:
        rule = AuthRule(
            name="jwt_no_query",
            rule_type="jwt",
            rule_config={},
        )
        result = rule.to_dgraph_rule()
        assert result == ""

    def test_graph_traversal_rule_returns_traversal(self) -> None:
        rule = AuthRule(
            name="traverse_auth",
            rule_type="graph_traversal",
            rule_config={"traversal": "~owns.uid"},
        )
        result = rule.to_dgraph_rule()
        assert result == "~owns.uid"

    def test_graph_traversal_missing_returns_empty(self) -> None:
        rule = AuthRule(
            name="traverse_no_config",
            rule_type="graph_traversal",
            rule_config={},
        )
        result = rule.to_dgraph_rule()
        assert result == ""

    def test_unknown_type_raises_value_error(self) -> None:
        rule = AuthRule(
            name="bad_type",
            rule_type="custom_unknown",
            rule_config={"key": "val"},
        )
        with pytest.raises(ValueError, match="Unknown auth rule type"):
            rule.to_dgraph_rule()
