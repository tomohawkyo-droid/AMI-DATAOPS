"""Tests for the secrets adapter ContextVar management.

Covers Issue #68 -- populate empty tests/unit/adapters/ directory,
and Issue #53 -- pointer_context cleanup.
"""

from ami.secrets.adapter import _POINTER_CONTEXT, pointer_context


class TestPointerContext:
    """Verify pointer_context clears the ContextVar on exit."""

    def test_context_clears_on_normal_exit(self) -> None:
        _POINTER_CONTEXT.set("should-be-cleared")

        with pointer_context():
            # On entry the context is cleared
            assert _POINTER_CONTEXT.get(None) is None
            _POINTER_CONTEXT.set("inside")

        # On exit the context is cleared again
        assert _POINTER_CONTEXT.get(None) is None

    def test_context_clears_after_exception(self) -> None:
        """Verify finally clause resets even when exception propagates."""
        _POINTER_CONTEXT.set("before")

        exc_propagated = False
        try:
            _run_context_with_error()
        except RuntimeError:
            exc_propagated = True

        assert exc_propagated
        assert _POINTER_CONTEXT.get(None) is None


def _run_context_with_error() -> None:
    with pointer_context():
        _POINTER_CONTEXT.set("active")
        msg = "boom"
        raise RuntimeError(msg)
