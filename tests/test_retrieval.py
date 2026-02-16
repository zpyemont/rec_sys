"""Tests for brand-partitioned retrieval queries."""
import pytest
from unittest.mock import AsyncMock

from app.ranker.retrieval import get_fresh_candidates, get_trending_candidates, get_candidates_explore_exploit
from app.ranker.session_embedding import get_explore_epsilon, determine_cold_start_stage, STAGE_BROWSING


@pytest.fixture
def mock_redis():
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)  # cache miss
    redis.setex = AsyncMock()
    return redis


@pytest.fixture
def mock_pg():
    pg = AsyncMock()
    return pg


@pytest.mark.asyncio
async def test_fresh_candidates_brand_partitioned(mock_redis, mock_pg):
    """Fresh query should use ROW_NUMBER PARTITION BY brand to cap per-brand."""
    mock_pg.fetch_val_list = AsyncMock(return_value=[
        "a1", "a2", "a3", "a4", "a5",
        "b1", "b2", "b3", "b4", "b5",
        "c1", "c2", "c3", "c4", "c5",
    ])

    result = await get_fresh_candidates(mock_redis, mock_pg, limit=150)

    call_args = mock_pg.fetch_val_list.call_args
    sql = call_args[0][0]
    assert "PARTITION BY" in sql.upper()
    assert "ROW_NUMBER" in sql.upper()
    assert len(result) <= 150


@pytest.mark.asyncio
async def test_fresh_candidates_uses_cache(mock_redis, mock_pg):
    """Should return cached results when available."""
    mock_redis.get = AsyncMock(return_value=["cached1", "cached2"])

    result = await get_fresh_candidates(mock_redis, mock_pg, limit=10)

    assert result == ["cached1", "cached2"]
    mock_pg.fetch_val_list.assert_not_called()


@pytest.mark.asyncio
async def test_trending_candidates_brand_partitioned(mock_redis, mock_pg):
    """Trending query should use ROW_NUMBER PARTITION BY brand."""
    mock_pg.fetch_val_list = AsyncMock(return_value=["t1", "t2", "t3"])

    result = await get_trending_candidates(mock_redis, mock_pg, limit=100)

    call_args = mock_pg.fetch_val_list.call_args
    sql = call_args[0][0]
    assert "PARTITION BY" in sql.upper()
    assert "like_count" in sql.lower()


def test_get_explore_epsilon_anonymous():
    """Anonymous users should get high exploration rate."""
    eps = get_explore_epsilon("brand_new", is_anonymous=True)
    assert eps == pytest.approx(0.4, abs=0.01)


def test_get_explore_epsilon_browsing():
    """Browsing users (session signals, no likes) get moderate exploration."""
    eps = get_explore_epsilon("browsing", is_anonymous=False)
    assert eps == pytest.approx(0.2, abs=0.01)


def test_get_explore_epsilon_established():
    """Established users get low exploration."""
    eps = get_explore_epsilon("established", is_anonymous=False)
    assert eps == pytest.approx(0.08, abs=0.01)


@pytest.mark.asyncio
async def test_explore_exploit_splits_candidates(mock_redis, mock_pg):
    """Explore/exploit should merge personalized + explore candidates."""
    exploit_ids = [f"exploit_{i}" for i in range(80)]
    explore_ids = [f"explore_{i}" for i in range(20)]

    mock_pg.fetch_val_list = AsyncMock(return_value=explore_ids)

    result = await get_candidates_explore_exploit(
        pg=mock_pg,
        redis=mock_redis,
        exploit_candidates=exploit_ids,
        epsilon=0.2,
        total_limit=100,
    )

    assert len(result) <= 100
    # Should have some explore items mixed in
    explore_in_result = [pid for pid in result if pid.startswith("explore_")]
    assert len(explore_in_result) > 0


def test_single_tap_triggers_browsing_stage():
    """A single positive session signal should trigger BROWSING stage (not BRAND_NEW)."""
    stage = determine_cold_start_stage(liked_count=0, session_positive_count=1)
    assert stage == STAGE_BROWSING
