"""Tests for ami.implementations.embedding_service."""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest

from ami.implementations.embedding_service import (
    EmbeddingService,
    get_embedding_service,
)

DEFAULT_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
MPNET_MODEL = "sentence-transformers/all-mpnet-base-v2"
ROBERTA_MODEL = "sentence-transformers/all-distilroberta-v1"
UNKNOWN_MODEL = "sentence-transformers/some-other-model"

DIM_SMALL = 384
DIM_LARGE = 768


@pytest.fixture(autouse=True)
def _inject_torch_mock() -> None:
    """Ensure a mock torch module is in sys.modules.

    The service imports torch inside sync methods, so the
    module must exist before those methods run.
    """
    mod = types.ModuleType("torch")
    mod.sum = MagicMock()
    mod.clamp = MagicMock()
    mod.nn = MagicMock()
    sys.modules["torch"] = mod


# -----------------------------------------------------------
# Embedding dimension lookup
# -----------------------------------------------------------


class TestEmbeddingDim:
    """Verify embedding_dim returns correct values per model."""

    def test_default_model_returns_384(self) -> None:
        svc = EmbeddingService(model_name=DEFAULT_MODEL)
        assert svc.embedding_dim == DIM_SMALL

    def test_mpnet_model_returns_768(self) -> None:
        svc = EmbeddingService(model_name=MPNET_MODEL)
        assert svc.embedding_dim == DIM_LARGE

    def test_distilroberta_returns_768(self) -> None:
        svc = EmbeddingService(model_name=ROBERTA_MODEL)
        assert svc.embedding_dim == DIM_LARGE

    def test_unknown_model_returns_768(self) -> None:
        svc = EmbeddingService(model_name=UNKNOWN_MODEL)
        assert svc.embedding_dim == DIM_LARGE


# -----------------------------------------------------------
# _extract_text_from_dict
# -----------------------------------------------------------


class TestExtractTextFromDict:
    """Verify recursive text extraction from dicts."""

    def _svc(self) -> EmbeddingService:
        return EmbeddingService(model_name=DEFAULT_MODEL)

    def test_string_values_included(self) -> None:
        result = self._svc()._extract_text_from_dict(
            {"title": "hello", "body": "world"},
        )
        assert "title: hello" in result
        assert "body: world" in result

    def test_list_of_strings_included(self) -> None:
        result = self._svc()._extract_text_from_dict(
            {"tags": ["alpha", "beta"]},
        )
        assert "alpha" in result
        assert "beta" in result

    def test_nested_dict_recursed(self) -> None:
        result = self._svc()._extract_text_from_dict(
            {"meta": {"author": "Ada"}},
        )
        assert "author: Ada" in result

    def test_empty_dict_returns_empty(self) -> None:
        result = self._svc()._extract_text_from_dict({})
        assert result == ""

    def test_non_string_values_skipped(self) -> None:
        result = self._svc()._extract_text_from_dict(
            {"count": 42, "flag": True},
        )
        assert result == ""


# -----------------------------------------------------------
# Singleton get_instance
# -----------------------------------------------------------


class TestGetInstance:
    """Verify the singleton factory on EmbeddingService."""

    @pytest.fixture(autouse=True)
    def _reset_singleton(self) -> None:
        EmbeddingService._instance = None

    def test_returns_singleton(self) -> None:
        a = EmbeddingService.get_instance()
        b = EmbeddingService.get_instance()
        assert a is b

    def test_same_model_returns_same_instance(self) -> None:
        a = EmbeddingService.get_instance(
            model_name=DEFAULT_MODEL,
        )
        b = EmbeddingService.get_instance(
            model_name=DEFAULT_MODEL,
        )
        assert a is b

    def test_different_model_creates_new(self) -> None:
        a = EmbeddingService.get_instance(
            model_name=DEFAULT_MODEL,
        )
        b = EmbeddingService.get_instance(
            model_name=MPNET_MODEL,
        )
        assert a is not b
        assert b.model_name == MPNET_MODEL


# -----------------------------------------------------------
# Convenience function
# -----------------------------------------------------------


class TestGetEmbeddingService:
    """Verify the module-level convenience function."""

    @pytest.fixture(autouse=True)
    def _reset_singleton(self) -> None:
        EmbeddingService._instance = None

    def test_returns_embedding_service_instance(self) -> None:
        svc = get_embedding_service()
        assert isinstance(svc, EmbeddingService)


# -----------------------------------------------------------
# _generate_embedding_sync validation
# -----------------------------------------------------------


class TestGenerateEmbeddingSyncEmpty:
    """Empty / whitespace input must raise ValueError."""

    @pytest.fixture(autouse=True)
    def _reset_singleton(self) -> None:
        EmbeddingService._instance = None

    def test_empty_string_raises(self) -> None:
        svc = EmbeddingService()
        with pytest.raises(ValueError, match="empty"):
            svc._generate_embedding_sync("")

    def test_whitespace_only_raises(self) -> None:
        svc = EmbeddingService()
        with pytest.raises(ValueError, match="whitespace"):
            svc._generate_embedding_sync("   ")


# -----------------------------------------------------------
# _generate_embeddings_sync validation
# -----------------------------------------------------------


class TestGenerateEmbeddingsSyncEmpty:
    """Empty list / empty-string items must raise ValueError."""

    @pytest.fixture(autouse=True)
    def _reset_singleton(self) -> None:
        EmbeddingService._instance = None

    def test_empty_list_raises(self) -> None:
        svc = EmbeddingService()
        with pytest.raises(ValueError, match="empty list"):
            svc._generate_embeddings_sync([])

    def test_list_with_empty_string_raises(self) -> None:
        svc = EmbeddingService()
        with pytest.raises(ValueError, match="empty"):
            svc._generate_embeddings_sync([""])
