import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.styling.tools import (
    generate_brief, search_products, inspect_product,
    check_compatibility, finalise, render_outfit_preview,
    _palette_overlap_score, _style_compatibility_score, _price_coherence_score,
)
from app.styling.schemas import OutfitBrief, ProductHit, ProductSummary, ProductDetail, CompatibilityReport, CandidateOutfit, RenderedPreview


def test_anthropic_client_importable():
    from app.styling.anthropic_client import get_anthropic_client
    assert get_anthropic_client is not None


class TestGenerateBrief:
    @pytest.fixture
    def mock_anthropic(self):
        client = MagicMock()
        message = MagicMock()
        message.content = [MagicMock(
            type="text",
            text='{"interpretation":"Dark academia","aesthetic_tags":["dark","academic"],'
                 '"palette":["black","ivory"],"formality":"smart-casual",'
                 '"occasion":"university","slots_needed":["top","bottom","shoes","bag"],'
                 '"notes":"avoid anything too casual"}'
        )]
        client.messages = MagicMock()
        client.messages.create = AsyncMock(return_value=message)
        return client

    @pytest.mark.asyncio
    async def test_returns_outfit_brief(self, mock_anthropic):
        result = await generate_brief("dark academia vibes", anthropic_client=mock_anthropic)
        assert isinstance(result, OutfitBrief)
        assert result.interpretation == "Dark academia"
        assert "black" in result.palette
        assert "top" in result.slots_needed

    @pytest.mark.asyncio
    async def test_calls_haiku_model(self, mock_anthropic):
        await generate_brief("any prompt", anthropic_client=mock_anthropic)
        call_kwargs = mock_anthropic.messages.create.call_args.kwargs
        assert "haiku" in call_kwargs["model"]

    @pytest.mark.asyncio
    async def test_invalid_json_raises_value_error(self):
        client = MagicMock()
        message = MagicMock()
        message.content = [MagicMock(type="text", text="not json")]
        client.messages = MagicMock()
        client.messages.create = AsyncMock(return_value=message)
        with pytest.raises(ValueError, match="Failed to parse brief"):
            await generate_brief("any prompt", anthropic_client=client)


class TestSearchProducts:
    @pytest.fixture
    def mock_pg(self):
        pg = MagicMock()
        pg.fetch_all = AsyncMock(return_value=[
            {
                "product_id": "domain:handle-1",
                "title": "Champagne Slip Dress",
                "price": 89.0,
                "currency": "GBP",
                "image_url": "https://example.com/img.jpg",
                "description": "Satin slip dress in champagne",
                "subcategory": "Dresses",
                "url": "https://example.com/product",
            }
        ])
        return pg

    @pytest.fixture
    def mock_embedding_service(self):
        svc = MagicMock()
        svc.get_embeddings = AsyncMock(return_value=([0.1] * 1024, [0.2] * 512))
        return svc

    @pytest.mark.asyncio
    async def test_returns_product_summaries(self, mock_pg, mock_embedding_service):
        results = await search_products(
            "dress",
            "champagne slip dress for summer wedding",
            pg=mock_pg,
            embedding_service=mock_embedding_service,
        )
        assert len(results) == 1
        assert isinstance(results[0], ProductHit)
        assert results[0].slot == "dress"
        assert results[0].product_id == "domain:handle-1"

    @pytest.mark.asyncio
    async def test_price_filter_applied_in_sql_args(self, mock_pg, mock_embedding_service):
        await search_products(
            "dress",
            "champagne slip dress",
            max_price_gbp=150.0,
            pg=mock_pg,
            embedding_service=mock_embedding_service,
        )
        assert mock_pg.fetch_all.called

    @pytest.mark.asyncio
    async def test_invalid_slot_raises(self, mock_pg, mock_embedding_service):
        with pytest.raises(ValueError, match="Unknown slot"):
            await search_products(
                "unicorn",
                "any description",
                pg=mock_pg,
                embedding_service=mock_embedding_service,
            )

    @pytest.mark.asyncio
    async def test_embedding_failure_raises(self, mock_pg):
        svc = MagicMock()
        svc.get_embeddings = AsyncMock(return_value=(None, None))
        with pytest.raises(RuntimeError, match="Embedding service"):
            await search_products("dress", "any", pg=mock_pg, embedding_service=svc)


class TestInspectProduct:
    @pytest.fixture
    def mock_pg_with_product(self):
        pg = MagicMock()
        pg.fetch_one = AsyncMock(return_value={
            "product_id": "domain:handle-1",
            "title": "Champagne Slip Dress",
            "price": 89.0,
            "currency": "GBP",
            "description": "Satin slip dress in champagne with adjustable straps",
            "subcategory": "Dresses",
            "url": "https://example.com/product",
        })
        pg.fetch_all = AsyncMock(return_value=[
            {"image_url": "https://example.com/img1.jpg"},
            {"image_url": "https://example.com/img2.jpg"},
        ])
        return pg

    @pytest.mark.asyncio
    async def test_returns_product_detail(self, mock_pg_with_product):
        result = await inspect_product("domain:handle-1", pg=mock_pg_with_product)
        assert isinstance(result, ProductDetail)
        assert result.product_id == "domain:handle-1"
        assert len(result.images) == 2
        assert result.slot == "dress"

    @pytest.mark.asyncio
    async def test_product_not_found_raises(self):
        pg = MagicMock()
        pg.fetch_one = AsyncMock(return_value=None)
        pg.fetch_all = AsyncMock(return_value=[])
        with pytest.raises(ValueError, match="Product not found"):
            await inspect_product("nonexistent", pg=pg)

    @pytest.mark.asyncio
    async def test_palette_extracted_from_description(self, mock_pg_with_product):
        result = await inspect_product("domain:handle-1", pg=mock_pg_with_product)
        assert "champagne" in result.palette


class TestInspectProductWithImage:
    @pytest.fixture
    def mock_pg_with_product(self):
        pg = MagicMock()
        pg.fetch_one = AsyncMock(return_value={
            "product_id": "domain:handle-1",
            "title": "Champagne Slip Dress",
            "price": 89.0,
            "currency": "GBP",
            "description": "Satin slip dress in champagne",
            "subcategory": "Dresses",
            "url": "https://example.com/product",
        })
        pg.fetch_all = AsyncMock(return_value=[
            {"image_url": "https://example.com/img1.jpg"},
        ])
        return pg

    @pytest.mark.asyncio
    async def test_returns_image_bytes_when_available(self, mock_pg_with_product):
        fake_img = b"\xff\xd8\xff" + b"\x00" * 100
        with patch("app.styling.tools.fetch_and_resize", new=AsyncMock(return_value=fake_img)):
            result = await inspect_product("domain:handle-1", pg=mock_pg_with_product)
        assert result.image_bytes is not None
        assert result.image_available is True

    @pytest.mark.asyncio
    async def test_falls_back_to_text_when_fetch_fails(self, mock_pg_with_product):
        with patch("app.styling.tools.fetch_and_resize", new=AsyncMock(return_value=None)):
            result = await inspect_product("domain:handle-1", pg=mock_pg_with_product)
        assert result.image_bytes is None
        assert result.image_available is False


class TestCompatibilityScores:
    def test_palette_overlap_same_colours(self):
        assert _palette_overlap_score(["black", "ivory"], ["black", "ivory"]) == 1.0

    def test_palette_overlap_no_colours(self):
        assert _palette_overlap_score(["black"], ["white"]) == 0.0

    def test_style_compatibility_adjacent(self):
        score = _style_compatibility_score("Dresses", "Heeled Sandals")
        assert score > 0.0

    def test_style_compatibility_unrelated(self):
        assert _style_compatibility_score("Dresses", "Basketball Shoes") == 0.0

    def test_price_coherence_same_price(self):
        assert _price_coherence_score([100.0, 100.0, 100.0]) == 1.0

    def test_price_coherence_mixed_tiers(self):
        assert _price_coherence_score([15.0, 500.0]) < 0.5

    def test_price_coherence_single_item(self):
        assert _price_coherence_score([100.0]) == 1.0


class TestCheckCompatibility:
    @pytest.fixture
    def mock_pg_multi(self):
        pg = MagicMock()
        pg.fetch_all = AsyncMock(return_value=[
            {"product_id": "p1", "title": "Black Midi Dress", "description": "black elegant midi dress", "subcategory": "Dresses", "price": 120.0},
            {"product_id": "p2", "title": "Black Heeled Sandals", "description": "black strappy sandals", "subcategory": "Heeled Sandals", "price": 85.0},
        ])
        return pg

    @pytest.fixture
    def mock_anthropic_compat(self):
        client = MagicMock()
        msg = MagicMock()
        msg.content = [MagicMock(type="text", text="These items work together: black palette creates coherence.")]
        client.messages = MagicMock()
        client.messages.create = AsyncMock(return_value=msg)
        return client

    @pytest.mark.asyncio
    async def test_returns_report(self, mock_pg_multi, mock_anthropic_compat):
        result = await check_compatibility(
            ["p1", "p2"],
            pg=mock_pg_multi,
            anthropic_client=mock_anthropic_compat,
        )
        assert isinstance(result, CompatibilityReport)
        assert 0.0 <= result.score <= 1.0
        assert result.rationale != ""

    @pytest.mark.asyncio
    async def test_compatible_palette(self, mock_pg_multi, mock_anthropic_compat):
        result = await check_compatibility(
            ["p1", "p2"],
            pg=mock_pg_multi,
            anthropic_client=mock_anthropic_compat,
        )
        assert result.palette_overlap > 0.5

    @pytest.mark.asyncio
    async def test_requires_at_least_two_products(self, mock_anthropic_compat):
        pg = MagicMock()
        pg.fetch_all = AsyncMock(return_value=[])
        with pytest.raises(ValueError, match="at least 2"):
            await check_compatibility(["p1"], pg=pg, anthropic_client=mock_anthropic_compat)


class TestSearchProductsWithReferenceImage:
    @pytest.fixture
    def mock_pg_with_embeddings(self):
        pg = MagicMock()
        pg.fetch_all = AsyncMock(side_effect=[
            # First call: fetch reference product embeddings
            [{"image_embedding": [0.5] * 512}],
            # Second call: search results
            [{
                "product_id": "p2",
                "title": "Black Heeled Sandals",
                "price": 85.0,
                "currency": "GBP",
                "image_url": "https://example.com/shoes.jpg",
                "description": "black strappy sandals",
                "subcategory": "Heeled Sandals",
                "url": "https://example.com/product2",
            }],
        ])
        return pg

    @pytest.mark.asyncio
    async def test_fetches_reference_embeddings(self, mock_pg_with_embeddings):
        svc = MagicMock()
        svc.get_embeddings = AsyncMock(return_value=([0.1] * 1024, [0.3] * 512))

        results = await search_products(
            "shoes",
            "heeled sandals to match the dress",
            reference_product_ids=["hero-dress-id"],
            pg=mock_pg_with_embeddings,
            embedding_service=svc,
        )
        assert len(results) == 1
        assert isinstance(results[0], ProductHit)
        assert mock_pg_with_embeddings.fetch_all.call_count == 2

    @pytest.mark.asyncio
    async def test_falls_back_to_text_only_when_no_reference_embeddings(self):
        pg = MagicMock()
        pg.fetch_all = AsyncMock(side_effect=[
            [],  # No embeddings found for reference
            [{"product_id": "p1", "title": "Shoes", "price": 50.0, "currency": "GBP",
              "image_url": "https://example.com/s.jpg", "description": "shoes",
              "subcategory": "Heeled Sandals", "url": "https://example.com/p1"}],
        ])
        svc = MagicMock()
        svc.get_embeddings = AsyncMock(return_value=([0.1] * 1024, [0.3] * 512))
        results = await search_products(
            "shoes", "heeled sandals",
            reference_product_ids=["missing-id"],
            pg=pg, embedding_service=svc,
        )
        assert len(results) == 1


class TestFinalise:
    def test_returns_none(self):
        assert finalise([]) is None

    def test_accepts_candidate_outfits(self):
        outfit = CandidateOutfit(outfit_id="o1", items=[], rationale="test", total_price_gbp=0.0)
        assert finalise([outfit]) is None


_GCS_URL = "https://storage.googleapis.com/looksy-outfit-composites/styling/composites/abc.png"


class TestRenderOutfitPreview:
    @pytest.fixture
    def mock_pg_images(self):
        pg = MagicMock()
        pg.fetch_all = AsyncMock(side_effect=[
            # image URL query
            [
                {"product_id": "p1", "image_url": "https://example.com/img1.jpg"},
                {"product_id": "p2", "image_url": "https://example.com/img2.jpg"},
            ],
            # subcategory query
            [
                {"product_id": "p1", "subcategory": "Dresses"},
                {"product_id": "p2", "subcategory": "Heeled Sandals"},
            ],
        ])
        return pg

    @pytest.fixture
    def mock_gcs(self):
        gcs = MagicMock()
        gcs.upload_bytes = MagicMock(return_value=_GCS_URL)
        return gcs

    @pytest.fixture
    def mock_redis(self):
        redis = MagicMock()
        redis.get = AsyncMock(return_value=None)
        redis.setex = AsyncMock()
        return redis

    @pytest.mark.asyncio
    async def test_returns_rendered_preview(self, mock_pg_images, mock_gcs, mock_redis):
        fake_png = b"\x89PNG" + b"\x00" * 100
        with (
            patch("app.styling.tools.fetch_and_resize", new=AsyncMock(return_value=b"\xff\xd8" + b"\x00" * 50)),
            patch("app.styling.compositor.build_composite", new=AsyncMock(return_value=fake_png)),
        ):
            result = await render_outfit_preview(
                ["p1", "p2"],
                pg=mock_pg_images,
                gcs=mock_gcs,
                redis=mock_redis,
            )
        assert isinstance(result, RenderedPreview)
        assert result.image_url == _GCS_URL
        assert result.image_bytes == fake_png

    @pytest.mark.asyncio
    async def test_uses_cache_on_second_call(self, mock_pg_images, mock_gcs, mock_redis):
        import json
        cached_url = "https://cached.example.com/img.png"
        cached_data = json.dumps({"image_url": cached_url, "layout_notes": "cached", "partial": False})
        mock_redis.get = AsyncMock(return_value=cached_data)
        fake_png = b"\x89PNG" + b"\x00" * 100

        result = await render_outfit_preview(
            ["p1", "p2"],
            pg=mock_pg_images,
            gcs=mock_gcs,
            redis=mock_redis,
            cached_image_bytes=fake_png,
        )
        assert result.image_url == cached_url

    @pytest.mark.asyncio
    async def test_partial_result_when_composite_fails(self, mock_pg_images, mock_gcs, mock_redis):
        with (
            patch("app.styling.tools.fetch_and_resize", new=AsyncMock(return_value=None)),
            patch("app.styling.compositor.build_composite", new=AsyncMock(return_value=None)),
        ):
            result = await render_outfit_preview(
                ["p1", "p2"],
                pg=mock_pg_images,
                gcs=mock_gcs,
                redis=mock_redis,
            )
        assert result.partial is True
        assert result.image_url == ""


class TestCheckCompatibilityWithVision:
    def _make_pg(self, rows):
        pg = MagicMock()
        pg.fetch_all = AsyncMock(return_value=rows)
        return pg

    def _make_anthropic(self, text="These work together beautifully."):
        client = MagicMock()
        msg = MagicMock()
        msg.content = [MagicMock(type="text", text=text)]
        client.messages = MagicMock()
        client.messages.create = AsyncMock(return_value=msg)
        return client

    def _default_rows(self):
        return [
            {"product_id": "p1", "title": "Black Dress", "description": "black midi dress", "subcategory": "Dresses", "price": 120.0},
            {"product_id": "p2", "title": "Black Heels", "description": "black heeled sandals", "subcategory": "Heeled Sandals", "price": 85.0},
        ]

    @pytest.mark.asyncio
    async def test_includes_composite_url_in_report(self):
        mock_preview = MagicMock()
        mock_preview.image_url = _GCS_URL
        mock_preview.image_bytes = b"\x89PNG" + b"\x00" * 100
        mock_preview.partial = False

        gcs = MagicMock()
        redis = MagicMock()
        redis.get = AsyncMock(return_value=None)
        redis.setex = AsyncMock()

        with patch("app.styling.tools.render_outfit_preview", new=AsyncMock(return_value=mock_preview)):
            result = await check_compatibility(
                ["p1", "p2"],
                pg=self._make_pg(self._default_rows()),
                anthropic_client=self._make_anthropic(),
                gcs=gcs,
                redis=redis,
            )
        assert result.preview_image_url == _GCS_URL

    @pytest.mark.asyncio
    async def test_falls_back_to_text_only_when_composite_fails(self):
        mock_preview = MagicMock()
        mock_preview.image_url = ""
        mock_preview.image_bytes = b""
        mock_preview.partial = True

        gcs = MagicMock()
        redis = MagicMock()
        redis.get = AsyncMock(return_value=None)
        redis.setex = AsyncMock()

        with patch("app.styling.tools.render_outfit_preview", new=AsyncMock(return_value=mock_preview)):
            result = await check_compatibility(
                ["p1", "p2"],
                pg=self._make_pg(self._default_rows()),
                anthropic_client=self._make_anthropic("These work together."),
                gcs=gcs,
                redis=redis,
            )
        assert not result.preview_image_url
        assert result.rationale != ""
