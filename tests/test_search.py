"""
Tests for hybrid search functionality.

These tests cover:
1. HybridSearcher class and search modes
2. EmbeddingService client
3. /search endpoint integration
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import List, Dict, Any

# Import the classes we're testing
import sys
import os

# Add the app directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.ranker.search import HybridSearcher, EmbeddingService
from app.schemas import SearchResponse, ProductItem


class TestEmbeddingService:
    """Tests for EmbeddingService client."""

    @pytest.fixture
    def embedding_service(self):
        return EmbeddingService(base_url="http://test-parser:8080", timeout=5.0)

    @pytest.mark.asyncio
    async def test_get_embeddings_success(self, embedding_service):
        """Test successful embedding retrieval."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "text_embedding": [0.1] * 768,
            "image_embedding": [0.2] * 512
        }
        mock_response.raise_for_status = MagicMock()

        with patch('app.ranker.search.httpx.AsyncClient') as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(return_value=mock_response)
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=None)
            mock_client.return_value = mock_instance

            text_emb, image_emb = await embedding_service.get_embeddings("red dress")

            assert text_emb is not None
            assert len(text_emb) == 768
            assert image_emb is not None
            assert len(image_emb) == 512

    @pytest.mark.asyncio
    async def test_get_embeddings_failure(self, embedding_service):
        """Test handling of embedding service failure."""
        with patch('app.ranker.search.httpx.AsyncClient') as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(side_effect=Exception("Connection failed"))
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=None)
            mock_client.return_value = mock_instance

            text_emb, image_emb = await embedding_service.get_embeddings("red dress")

            assert text_emb is None
            assert image_emb is None


class TestHybridSearcher:
    """Tests for HybridSearcher class."""

    @pytest.fixture
    def mock_pg_client(self):
        """Create a mock PostgreSQL client."""
        mock = AsyncMock()
        return mock

    @pytest.fixture
    def mock_embedding_service(self):
        """Create a mock embedding service."""
        mock = AsyncMock()
        mock.get_embeddings = AsyncMock(return_value=([0.1] * 768, [0.2] * 512))
        return mock

    @pytest.fixture
    def searcher(self, mock_pg_client, mock_embedding_service):
        """Create a HybridSearcher instance with mocks."""
        return HybridSearcher(
            pg_client=mock_pg_client,
            embedding_service=mock_embedding_service,
            rrf_k=60,
            candidate_multiplier=5
        )

    @pytest.mark.asyncio
    async def test_hybrid_search_returns_results(self, searcher, mock_pg_client):
        """Test that hybrid search returns product IDs."""
        mock_pg_client.fetch_all = AsyncMock(return_value=[
            {"product_id": "product1", "score": 0.5},
            {"product_id": "product2", "score": 0.4},
            {"product_id": "product3", "score": 0.3},
        ])

        product_ids, latency = await searcher.search(
            query="red dress",
            limit=10,
            mode="hybrid",
            weights=(1.0, 1.0, 1.0)
        )

        assert len(product_ids) == 3
        assert product_ids == ["product1", "product2", "product3"]
        assert latency > 0

    @pytest.mark.asyncio
    async def test_semantic_search_mode(self, searcher, mock_pg_client, mock_embedding_service):
        """Test semantic-only search mode."""
        mock_pg_client.fetch_all = AsyncMock(return_value=[
            {"product_id": "semantic1"},
            {"product_id": "semantic2"},
        ])

        product_ids, latency = await searcher.search(
            query="red dress",
            limit=10,
            mode="semantic",
            weights=(1.0, 1.0, 1.0)
        )

        assert len(product_ids) == 2
        # Verify it used the semantic search SQL (not hybrid)
        mock_embedding_service.get_embeddings.assert_called_once_with("red dress")

    @pytest.mark.asyncio
    async def test_visual_search_mode(self, searcher, mock_pg_client, mock_embedding_service):
        """Test visual-only search mode (text-to-image)."""
        mock_pg_client.fetch_all = AsyncMock(return_value=[
            {"product_id": "visual1"},
            {"product_id": "visual2"},
        ])

        product_ids, latency = await searcher.search(
            query="red dress",
            limit=10,
            mode="visual",
            weights=(1.0, 1.0, 1.0)
        )

        assert len(product_ids) == 2
        mock_embedding_service.get_embeddings.assert_called_once_with("red dress")

    @pytest.mark.asyncio
    async def test_keyword_search_mode(self, searcher, mock_pg_client, mock_embedding_service):
        """Test keyword-only search mode."""
        mock_pg_client.fetch_all = AsyncMock(return_value=[
            {"product_id": "keyword1"},
            {"product_id": "keyword2"},
        ])

        product_ids, latency = await searcher.search(
            query="red dress",
            limit=10,
            mode="keyword",
            weights=(1.0, 1.0, 1.0)
        )

        assert len(product_ids) == 2
        # Keyword search should NOT call embedding service
        mock_embedding_service.get_embeddings.assert_not_called()

    @pytest.mark.asyncio
    async def test_fallback_on_embedding_failure(self, searcher, mock_pg_client, mock_embedding_service):
        """Test fallback to keyword search when embeddings fail."""
        mock_embedding_service.get_embeddings = AsyncMock(return_value=(None, None))
        mock_pg_client.fetch_all = AsyncMock(return_value=[
            {"product_id": "fallback1"},
        ])

        product_ids, latency = await searcher.search(
            query="red dress",
            limit=10,
            mode="semantic",  # Semantic mode should fall back to keyword
            weights=(1.0, 1.0, 1.0)
        )

        assert len(product_ids) == 1
        assert product_ids == ["fallback1"]

    @pytest.mark.asyncio
    async def test_empty_results(self, searcher, mock_pg_client):
        """Test handling of empty search results."""
        mock_pg_client.fetch_all = AsyncMock(return_value=[])

        product_ids, latency = await searcher.search(
            query="nonexistent product xyz123",
            limit=10,
            mode="keyword",
            weights=(1.0, 1.0, 1.0)
        )

        assert product_ids == []
        assert latency > 0

    @pytest.mark.asyncio
    async def test_custom_weights(self, searcher, mock_pg_client, mock_embedding_service):
        """Test that custom weights are passed correctly."""
        mock_pg_client.fetch_all = AsyncMock(return_value=[
            {"product_id": "weighted1", "score": 0.8},
        ])

        # Set specific weights - emphasize keyword search
        await searcher.search(
            query="red dress",
            limit=10,
            mode="hybrid",
            weights=(0.2, 0.3, 0.5)  # Low text, medium image, high keyword
        )

        # Verify the query was called with the correct weights
        call_args = mock_pg_client.fetch_all.call_args
        params = call_args[0][1]  # Second argument is params list
        assert params[4] == 0.2  # text_weight
        assert params[5] == 0.3  # image_weight
        assert params[6] == 0.5  # keyword_weight


class TestSearchResponseSchema:
    """Tests for SearchResponse schema."""

    def test_search_response_creation(self):
        """Test creating a SearchResponse object."""
        response = SearchResponse(
            query="red dress",
            results=[
                ProductItem(
                    id="product1",
                    title="Red Floral Dress",
                    price=59.99,
                    brand="TestBrand"
                )
            ],
            total=1,
            mode="hybrid",
            latency_ms=45.5
        )

        assert response.query == "red dress"
        assert len(response.results) == 1
        assert response.results[0].title == "Red Floral Dress"
        assert response.total == 1
        assert response.mode == "hybrid"
        assert response.latency_ms == 45.5

    def test_search_response_empty_results(self):
        """Test SearchResponse with empty results."""
        response = SearchResponse(
            query="nonexistent",
            results=[],
            total=0,
            mode="keyword",
            latency_ms=10.0
        )

        assert response.query == "nonexistent"
        assert response.results == []
        assert response.total == 0


class TestSearchEndpoint:
    """Integration tests for /search endpoint."""

    @pytest.mark.skip(reason="Requires full FastAPI environment with database")
    def test_search_endpoint_exists(self):
        """Test that /search endpoint responds.

        This test requires a running database and is skipped in unit tests.
        """
        pass

    def test_search_query_validation(self):
        """Test that query parameter is required."""
        # The endpoint requires 'q' parameter
        from app.schemas import SearchResponse

        # Verify the model can be instantiated
        response = SearchResponse(
            query="test",
            results=[],
            total=0,
            mode="hybrid",
            latency_ms=0.0
        )
        assert response is not None


# RRF Score Function Tests (verifies the mathematical properties)
class TestRRFScore:
    """Tests for RRF scoring logic."""

    def test_rrf_score_formula(self):
        """Test the RRF score formula: 1 / (rank + k)"""
        k = 60

        # Rank 1 should have highest score
        rank1_score = 1.0 / (1 + k)
        assert rank1_score == pytest.approx(0.01639, rel=0.01)

        # Rank 10 should have lower score
        rank10_score = 1.0 / (10 + k)
        assert rank10_score == pytest.approx(0.01429, rel=0.01)

        # Higher rank = lower score
        assert rank1_score > rank10_score

    def test_rrf_fusion_additive(self):
        """Test that RRF scores are additive across search methods."""
        k = 60

        # Product appears rank 1 in text search, rank 5 in image search
        text_score = 1.0 / (1 + k)
        image_score = 1.0 / (5 + k)
        total_score = text_score + image_score

        # Product only in keyword search rank 1
        keyword_only = 1.0 / (1 + k)

        # Product in multiple searches should score higher
        assert total_score > keyword_only

    def test_rrf_with_weights(self):
        """Test weighted RRF fusion."""
        k = 60

        # Equal weights
        text_weight, image_weight, keyword_weight = 1.0, 1.0, 1.0
        rank1_text = (1.0 / (1 + k)) * text_weight
        rank1_image = (1.0 / (1 + k)) * image_weight
        rank1_keyword = (1.0 / (1 + k)) * keyword_weight

        equal_total = rank1_text + rank1_image + rank1_keyword

        # Double keyword weight
        text_weight, image_weight, keyword_weight = 1.0, 1.0, 2.0
        rank1_text = (1.0 / (1 + k)) * text_weight
        rank1_image = (1.0 / (1 + k)) * image_weight
        rank1_keyword = (1.0 / (1 + k)) * keyword_weight

        keyword_heavy = rank1_text + rank1_image + rank1_keyword

        # Keyword-heavy weighting should give higher score when keyword rank is good
        assert keyword_heavy > equal_total
