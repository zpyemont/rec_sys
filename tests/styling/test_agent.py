import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch
from app.styling.agent import run_styling_agent
from app.styling.schemas import StyleRequest, CandidateOutfit


def _make_tool_response(tool_name: str, tool_use_id: str, content: dict):
    msg = MagicMock()
    msg.stop_reason = "tool_use"
    tool_block = MagicMock()
    tool_block.type = "tool_use"
    tool_block.name = tool_name
    tool_block.id = tool_use_id
    tool_block.input = content
    msg.content = [tool_block]
    return msg


def _make_final_response():
    msg = MagicMock()
    msg.stop_reason = "tool_use"
    tool_block = MagicMock()
    tool_block.type = "tool_use"
    tool_block.name = "finalise"
    tool_block.id = "tu_fin"
    tool_block.input = {
        "outfits": [{
            "outfit_id": "o1",
            "items": [{
                "slot": "dress",
                "product_id": "p1",
                "title": "Black Midi Dress",
                "price_gbp": 120.0,
                "image_url": "https://example.com/img.jpg",
                "affiliate_url": "https://example.com/product",
                "match_reason": "Sleek silhouette",
            }],
            "rationale": "Elegant and cohesive",
            "total_price_gbp": 120.0,
        }]
    }
    msg.content = [tool_block]
    return msg


class TestAgentLoop:
    @pytest.fixture
    def mock_deps(self):
        pg = MagicMock()
        anthropic_client = MagicMock()
        embedding_service = MagicMock()
        redis = MagicMock()
        redis.get = AsyncMock(return_value=None)
        redis.setex = AsyncMock()
        return pg, anthropic_client, embedding_service, redis

    @pytest.mark.asyncio
    async def test_loop_terminates_on_finalise(self, mock_deps):
        pg, anthropic_client, embedding_service, redis = mock_deps
        anthropic_client.messages = MagicMock()
        anthropic_client.messages.create = AsyncMock(return_value=_make_final_response())

        request = StyleRequest(prompt="dark academia", num_outfits=1)
        events = []
        async for event in run_styling_agent(
            request=request,
            pg=pg,
            anthropic_client=anthropic_client,
            embedding_service=embedding_service,
            redis=redis,
        ):
            events.append(event)

        event_types = [e["type"] for e in events]
        assert "final" in event_types

    @pytest.mark.asyncio
    async def test_loop_respects_hard_cap(self, mock_deps):
        pg, anthropic_client, embedding_service, redis = mock_deps

        def always_search(**kwargs):
            return _make_tool_response("search_products", "tu_s", {"slot": "dress", "description": "black dress"})

        anthropic_client.messages = MagicMock()
        anthropic_client.messages.create = AsyncMock(side_effect=always_search)

        with patch("app.styling.agent.search_products", new=AsyncMock(return_value=[])):
            request = StyleRequest(prompt="test", num_outfits=1)
            events = []
            async for event in run_styling_agent(
                request=request,
                pg=pg,
                anthropic_client=anthropic_client,
                embedding_service=embedding_service,
                redis=redis,
            ):
                events.append(event)

        event_types = [e["type"] for e in events]
        assert "final" in event_types
        tool_starts = [e for e in events if e["type"] == "tool_call_start"]
        assert len(tool_starts) <= 25

    @pytest.mark.asyncio
    async def test_emits_tool_call_events(self, mock_deps):
        pg, anthropic_client, embedding_service, redis = mock_deps

        call_count = 0
        def one_search_then_finalise(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _make_tool_response("search_products", "tu_1", {"slot": "dress", "description": "test"})
            return _make_final_response()

        anthropic_client.messages = MagicMock()
        anthropic_client.messages.create = AsyncMock(side_effect=one_search_then_finalise)

        with patch("app.styling.agent.search_products", new=AsyncMock(return_value=[])):
            request = StyleRequest(prompt="test", num_outfits=1)
            events = []
            async for event in run_styling_agent(
                request=request,
                pg=pg,
                anthropic_client=anthropic_client,
                embedding_service=embedding_service,
                redis=redis,
            ):
                events.append(event)

        event_types = [e["type"] for e in events]
        assert "tool_call_start" in event_types
        assert "tool_call_end" in event_types

    @pytest.mark.asyncio
    async def test_human_field_present_on_tool_events(self, mock_deps):
        pg, anthropic_client, embedding_service, redis = mock_deps
        anthropic_client.messages = MagicMock()
        anthropic_client.messages.create = AsyncMock(return_value=_make_final_response())

        request = StyleRequest(prompt="test", num_outfits=1)
        events = []
        async for event in run_styling_agent(
            request=request,
            pg=pg,
            anthropic_client=anthropic_client,
            embedding_service=embedding_service,
            redis=redis,
        ):
            events.append(event)

        for e in events:
            if e["type"] in ("tool_call_start", "tool_call_end"):
                assert "human" in e, f"Missing 'human' field on event: {e}"
                assert isinstance(e["human"], str) and len(e["human"]) > 0

    @pytest.mark.asyncio
    async def test_inspect_product_image_stripped_from_history(self, mock_deps):
        """Image bytes should not accumulate in the conversation history."""
        pg, anthropic_client, embedding_service, redis = mock_deps

        call_count = 0
        def inspect_then_finalise(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _make_tool_response("inspect_product", "tu_1", {"product_id": "p1"})
            # On second call, verify no image blocks in messages
            messages = kwargs.get("messages", [])
            for msg in messages:
                if msg.get("role") == "user":
                    content = msg.get("content", [])
                    if isinstance(content, list):
                        for block in content:
                            if isinstance(block, dict) and block.get("type") == "tool_result":
                                result_content = block.get("content", [])
                                if isinstance(result_content, list):
                                    for cb in result_content:
                                        assert cb.get("type") != "image", "Image block found in history after stripping"
            return _make_final_response()

        anthropic_client.messages = MagicMock()
        anthropic_client.messages.create = AsyncMock(side_effect=inspect_then_finalise)

        fake_detail = MagicMock()
        fake_detail.title = "Test Dress"
        fake_detail.image_bytes = b"\xff\xd8\xff" + b"\x00" * 50
        fake_detail.image_available = True
        fake_detail.model_dump.return_value = {
            "product_id": "p1", "title": "Test Dress", "price_gbp": 100.0,
            "images": [], "description": "test", "materials": None, "fit_notes": None,
            "palette": [], "affiliate_url": "", "slot": "dress", "subcategory": "Dresses",
            "image_bytes": None, "image_available": True,
        }
        fake_detail.model_dump_json.return_value = "{}"

        with patch("app.styling.agent.inspect_product", new=AsyncMock(return_value=fake_detail)):
            request = StyleRequest(prompt="test", num_outfits=1)
            events = []
            async for event in run_styling_agent(
                request=request, pg=pg, anthropic_client=anthropic_client,
                embedding_service=embedding_service, redis=redis,
            ):
                events.append(event)

        assert any(e["type"] == "final" for e in events)


class TestStyleEndpoint:
    def test_style_endpoint_exists(self):
        from app.main import app
        from fastapi.testclient import TestClient
        client = TestClient(app)
        r = client.post("/style", json={})
        assert r.status_code != 404

    def test_style_requires_prompt(self):
        from app.main import app
        from fastapi.testclient import TestClient
        client = TestClient(app)
        r = client.post("/style", json={})
        assert r.status_code == 422


class TestSwapEndpoint:
    def test_swap_endpoint_exists(self):
        from app.main import app
        from fastapi.testclient import TestClient
        client = TestClient(app)
        r = client.post("/style/swap", json={})
        assert r.status_code != 404

    def test_swap_requires_fields(self):
        from app.main import app
        from fastapi.testclient import TestClient
        client = TestClient(app)
        r = client.post("/style/swap", json={})
        assert r.status_code == 422


class TestVCREndToEnd:
    def test_vcr_dark_academia_produces_final_event(self):
        import pathlib
        import pytest
        fixture_path = pathlib.Path(__file__).parent / "fixtures" / "vcr_dark_academia.json"
        if not fixture_path.exists():
            pytest.skip("VCR fixture not yet recorded")
        events = json.loads(fixture_path.read_text())
        event_types = [e["type"] for e in events]
        assert "final" in event_types
        final = next(e for e in events if e["type"] == "final")
        assert len(final.get("outfits", [])) >= 1
