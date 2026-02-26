"""Test-only embedding service that returns deterministic vectors."""

from ami.implementations.embedding_service import EmbeddingService


class TestEmbeddingService(EmbeddingService):
    """Test-only embedding service that returns deterministic vectors."""

    def __init__(self, embedding_dim: int = 32) -> None:
        super().__init__(model_name="test")
        self._embedding_dim_override = embedding_dim

    @property
    def embedding_dim(self) -> int:
        return self._embedding_dim_override

    def _get_model(self) -> tuple[None, None]:
        """Return None (avoid loading external models)."""
        return None, None

    def _generate_embedding_sync(self, text: str) -> list[float]:
        """Generate deterministic vector derived from input length."""
        if not text or not text.strip():
            msg = "Cannot generate embedding for empty or whitespace-only text"
            raise ValueError(msg)
        vector = [0.0] * self.embedding_dim
        vector[0] = float(len(text))
        if self.embedding_dim > 1:
            vector[1] = 1.0
        return vector

    def _generate_embeddings_sync(self, texts: list[str]) -> list[list[float]]:
        """Generate deterministic vectors for all entries."""
        return [self._generate_embedding_sync(text) for text in texts]


def build_test_embedding_service(
    embedding_dim: int = 32,
) -> TestEmbeddingService:
    """Factory helper for tests that require deterministic embeddings."""
    return TestEmbeddingService(embedding_dim=embedding_dim)
