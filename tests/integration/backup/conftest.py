"""Pytest configuration for backup integration tests.

Path setup is handled by tests/conftest.py (inherited automatically).
"""

import pytest


@pytest.fixture(autouse=True)
def disable_file_locking(monkeypatch):
    """Disable file locking for all backup tests."""
    monkeypatch.setenv("AMI_TEST_MODE", "1")
