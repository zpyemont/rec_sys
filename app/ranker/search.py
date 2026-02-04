"""
Hybrid search implementation using RRF (Reciprocal Rank Fusion).

Combines three search methods:
1. Semantic text search (Marqo embeddings - 768-dim)
2. Visual search (CLIP embeddings - 512-dim, supports text-to-image)
3. Keyword search (PostgreSQL tsvector full-text search)
"""

import logging
import time
from typing import List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)


def _to_pgvector(embedding: List[float]) -> str:
    """Convert embedding list to pgvector string format."""
    return "[" + ",".join(str(x) for x in embedding) + "]"


class EmbeddingService:
    """Client for the parser embedding service."""

    def __init__(self, base_url: str, timeout: float = 10.0):
        """
        Initialize embedding service client.

        Args:
            base_url: URL of the parser embedding service (e.g., "http://parser:8080")
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    async def get_embeddings(self, query: str) -> Tuple[Optional[List[float]], Optional[List[float]]]:
        """
        Get text and CLIP embeddings for a query.

        Args:
            query: Search query text

        Returns:
            Tuple of (text_embedding, image_embedding)
            Each can be None if generation fails
        """
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.base_url}/embed",
                    json={"query": query}
                )
                response.raise_for_status()
                data = response.json()
                return data.get("text_embedding"), data.get("image_embedding")

        except Exception as e:
            logger.error(f"Failed to get embeddings from service: {e}")
            return None, None


class HybridSearcher:
    """
    Hybrid search using RRF (Reciprocal Rank Fusion).

    Combines semantic text search, visual search, and keyword search
    with configurable weights for each method.
    """

    # SQL for hybrid search with RRF fusion
    HYBRID_SEARCH_SQL = """
        WITH
        text_search AS (
            SELECT e.product_id,
                   ROW_NUMBER() OVER (ORDER BY e.text_embedding <=> $1::vector) as rank
            FROM embeddings.product_vectors e
            JOIN catalog.product_pricing pr ON e.product_id = pr.product_id
            WHERE pr.is_active = true AND e.text_embedding IS NOT NULL
            ORDER BY e.text_embedding <=> $1::vector
            LIMIT $4
        ),
        image_search AS (
            SELECT e.product_id,
                   ROW_NUMBER() OVER (ORDER BY e.image_embedding <=> $2::vector) as rank
            FROM embeddings.product_vectors e
            JOIN catalog.product_pricing pr ON e.product_id = pr.product_id
            WHERE pr.is_active = true AND e.image_embedding IS NOT NULL
            ORDER BY e.image_embedding <=> $2::vector
            LIMIT $4
        ),
        keyword_search AS (
            SELECT p.product_id,
                   ROW_NUMBER() OVER (ORDER BY ts_rank_cd(p.search_vector, websearch_to_tsquery('english', $3)) DESC) as rank
            FROM catalog.products p
            JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
            WHERE pr.is_active = true
              AND p.search_vector @@ websearch_to_tsquery('english', $3)
            ORDER BY ts_rank_cd(p.search_vector, websearch_to_tsquery('english', $3)) DESC
            LIMIT $4
        )
        SELECT
            COALESCE(t.product_id, i.product_id, k.product_id) as product_id,
            (COALESCE(rrf_score(t.rank), 0) * $5 +
             COALESCE(rrf_score(i.rank), 0) * $6 +
             COALESCE(rrf_score(k.rank), 0) * $7) as score
        FROM text_search t
        FULL OUTER JOIN image_search i ON t.product_id = i.product_id
        FULL OUTER JOIN keyword_search k ON COALESCE(t.product_id, i.product_id) = k.product_id
        ORDER BY score DESC
        LIMIT $8
    """

    # SQL for semantic text search only
    SEMANTIC_SEARCH_SQL = """
        SELECT e.product_id
        FROM embeddings.product_vectors e
        JOIN catalog.product_pricing pr ON e.product_id = pr.product_id
        WHERE pr.is_active = true AND e.text_embedding IS NOT NULL
        ORDER BY e.text_embedding <=> $1::vector
        LIMIT $2
    """

    # SQL for visual search only (CLIP text-to-image)
    VISUAL_SEARCH_SQL = """
        SELECT e.product_id
        FROM embeddings.product_vectors e
        JOIN catalog.product_pricing pr ON e.product_id = pr.product_id
        WHERE pr.is_active = true AND e.image_embedding IS NOT NULL
        ORDER BY e.image_embedding <=> $1::vector
        LIMIT $2
    """

    # SQL for keyword search only
    KEYWORD_SEARCH_SQL = """
        SELECT p.product_id
        FROM catalog.products p
        JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
        WHERE pr.is_active = true
          AND p.search_vector @@ websearch_to_tsquery('english', $1)
        ORDER BY ts_rank_cd(p.search_vector, websearch_to_tsquery('english', $1)) DESC
        LIMIT $2
    """

    def __init__(
        self,
        pg_client,  # AsyncPostgresClient
        embedding_service: EmbeddingService,
        rrf_k: int = 60,
        candidate_multiplier: int = 5,
    ):
        """
        Initialize hybrid searcher.

        Args:
            pg_client: Async PostgreSQL client
            embedding_service: Service for generating query embeddings
            rrf_k: RRF constant (lower = more weight to top ranks)
            candidate_multiplier: Fetch this many times more candidates before fusion
        """
        self.pg = pg_client
        self.embedding_service = embedding_service
        self.rrf_k = rrf_k
        self.candidate_multiplier = candidate_multiplier

    async def search(
        self,
        query: str,
        limit: int = 20,
        mode: str = "hybrid",
        weights: Tuple[float, float, float] = (1.0, 1.0, 1.0),
    ) -> Tuple[List[str], float]:
        """
        Execute hybrid search.

        Args:
            query: Search query text
            limit: Maximum number of results to return
            mode: Search mode - "hybrid", "semantic", "visual", "keyword"
            weights: Tuple of (text_weight, image_weight, keyword_weight) for RRF fusion

        Returns:
            Tuple of (list of product_ids, latency in milliseconds)
        """
        start_time = time.time()

        if mode == "keyword":
            product_ids = await self._keyword_search(query, limit)
        elif mode == "semantic":
            text_emb, _ = await self.embedding_service.get_embeddings(query)
            if text_emb is None:
                logger.warning("Failed to get text embedding, falling back to keyword search")
                product_ids = await self._keyword_search(query, limit)
            else:
                product_ids = await self._semantic_search(text_emb, limit)
        elif mode == "visual":
            _, image_emb = await self.embedding_service.get_embeddings(query)
            if image_emb is None:
                logger.warning("Failed to get CLIP embedding, falling back to keyword search")
                product_ids = await self._keyword_search(query, limit)
            else:
                product_ids = await self._visual_search(image_emb, limit)
        else:  # hybrid
            product_ids = await self._hybrid_search(query, limit, weights)

        latency_ms = (time.time() - start_time) * 1000
        logger.info(f"Search completed: mode={mode}, results={len(product_ids)}, latency={latency_ms:.1f}ms")

        return product_ids, latency_ms

    async def _hybrid_search(
        self,
        query: str,
        limit: int,
        weights: Tuple[float, float, float],
    ) -> List[str]:
        """Execute full hybrid search with RRF fusion."""
        # Get embeddings
        text_emb, image_emb = await self.embedding_service.get_embeddings(query)

        if text_emb is None and image_emb is None:
            # Fall back to keyword-only search
            logger.warning("No embeddings available, using keyword search only")
            return await self._keyword_search(query, limit)

        # If one embedding is missing, zero out its weight
        text_weight, image_weight, keyword_weight = weights
        if text_emb is None:
            text_weight = 0.0
            text_emb = [0.0] * 768  # Placeholder
        if image_emb is None:
            image_weight = 0.0
            image_emb = [0.0] * 512  # Placeholder

        candidate_limit = limit * self.candidate_multiplier

        rows = await self.pg.fetch_all(
            self.HYBRID_SEARCH_SQL,
            [
                _to_pgvector(text_emb),   # $1 - text embedding
                _to_pgvector(image_emb),  # $2 - image embedding
                query,                     # $3 - keyword query
                candidate_limit,           # $4 - per-method candidate limit
                text_weight,               # $5 - text search weight
                image_weight,              # $6 - image search weight
                keyword_weight,            # $7 - keyword search weight
                limit,                     # $8 - final result limit
            ]
        )

        return [row["product_id"] for row in rows]

    async def _semantic_search(self, text_embedding: List[float], limit: int) -> List[str]:
        """Execute semantic text search only."""
        rows = await self.pg.fetch_all(
            self.SEMANTIC_SEARCH_SQL,
            [_to_pgvector(text_embedding), limit]
        )
        return [row["product_id"] for row in rows]

    async def _visual_search(self, image_embedding: List[float], limit: int) -> List[str]:
        """Execute visual search (CLIP text-to-image) only."""
        rows = await self.pg.fetch_all(
            self.VISUAL_SEARCH_SQL,
            [_to_pgvector(image_embedding), limit]
        )
        return [row["product_id"] for row in rows]

    async def _keyword_search(self, query: str, limit: int) -> List[str]:
        """Execute keyword search only."""
        rows = await self.pg.fetch_all(
            self.KEYWORD_SEARCH_SQL,
            [query, limit]
        )
        return [row["product_id"] for row in rows]
