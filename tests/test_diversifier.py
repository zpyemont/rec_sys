"""Tests for MMR diversity re-ranking."""
import numpy as np
import pytest

from app.ranker.diversifier import mmr_rerank


def test_mmr_rerank_basic_diversity():
    """MMR should prefer diverse items over duplicate-style items."""
    embeddings = {
        "a1": np.array([1.0, 0.0, 0.0], dtype=np.float32),
        "a2": np.array([0.99, 0.01, 0.0], dtype=np.float32),
        "a3": np.array([0.98, 0.02, 0.0], dtype=np.float32),
        "b1": np.array([0.0, 1.0, 0.0], dtype=np.float32),
        "c1": np.array([0.0, 0.0, 1.0], dtype=np.float32),
    }
    relevance = {"a1": 1.0, "a2": 1.0, "a3": 1.0, "b1": 1.0, "c1": 1.0}
    candidates = ["a1", "a2", "a3", "b1", "c1"]

    result = mmr_rerank(candidates, embeddings, relevance, k=3, lambda_param=0.5)

    assert len(result) == 3
    assert "a1" in result
    assert "b1" in result or "c1" in result


def test_mmr_rerank_respects_relevance():
    """With high lambda, MMR should mostly follow relevance order."""
    embeddings = {
        "p1": np.array([1.0, 0.0], dtype=np.float32),
        "p2": np.array([0.0, 1.0], dtype=np.float32),
        "p3": np.array([0.5, 0.5], dtype=np.float32),
    }
    relevance = {"p1": 0.9, "p2": 0.5, "p3": 0.1}
    candidates = ["p1", "p2", "p3"]

    result = mmr_rerank(candidates, embeddings, relevance, k=3, lambda_param=0.95)

    assert result[0] == "p1"


def test_mmr_rerank_handles_missing_embeddings():
    """Items without embeddings should still be included (at end)."""
    embeddings = {
        "p1": np.array([1.0, 0.0], dtype=np.float32),
    }
    relevance = {"p1": 0.9, "p2": 0.8}
    candidates = ["p1", "p2"]

    result = mmr_rerank(candidates, embeddings, relevance, k=2, lambda_param=0.5)

    assert len(result) == 2
    assert "p1" in result
    assert "p2" in result


def test_mmr_rerank_empty_input():
    """Should handle empty candidate list."""
    result = mmr_rerank([], {}, {}, k=5, lambda_param=0.5)
    assert result == []
