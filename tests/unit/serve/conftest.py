"""Shared fixtures for serve unit tests."""

from __future__ import annotations

import os
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def serve_templates_dir() -> Path:
    """Absolute path to res/ansible/templates (the serve Jinja2 templates).

    Locates the DATAOPS project root by walking up from this file until
    pyproject.toml is found, so the fixture works regardless of cwd.
    """
    current = Path(__file__).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").is_file() and (
            candidate / "res" / "ansible"
        ).is_dir():
            return candidate / "res" / "ansible" / "templates"
    msg = f"Could not locate DATAOPS project root from {current}"
    raise RuntimeError(msg)


@pytest.fixture(scope="session", autouse=True)
def _ensure_not_hardcoded_home() -> None:
    """Ensure HOME is set so test state paths never fall back to /root."""
    os.environ.setdefault("HOME", os.path.expanduser("~"))
