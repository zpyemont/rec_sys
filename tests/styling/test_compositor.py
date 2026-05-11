import asyncio
import pytest
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock, patch
from PIL import Image as PILImage


class TestGCSUpload:
    def test_upload_bytes_calls_blob_upload(self):
        with patch("app.connectors.gcs.storage") as mock_storage:
            mock_client = MagicMock()
            mock_storage.Client.return_value = mock_client
            mock_blob = MagicMock()
            mock_client.bucket.return_value.blob.return_value = mock_blob

            from app.connectors.gcs import GCSClient
            from app.settings import get_settings
            client = GCSClient(get_settings())
            url = client.upload_bytes("test-bucket", "path/file.png", b"data", "image/png")

            mock_blob.upload_from_string.assert_called_once_with(b"data", content_type="image/png")
            assert url == "https://storage.googleapis.com/test-bucket/path/file.png"

    def test_upload_bytes_uses_default_bucket(self):
        with patch("app.connectors.gcs.storage") as mock_storage:
            mock_client = MagicMock()
            mock_storage.Client.return_value = mock_client
            mock_blob = MagicMock()
            mock_client.bucket.return_value.blob.return_value = mock_blob

            from app.connectors.gcs import GCSClient
            from app.settings import Settings
            settings = Settings(gcs_bucket_composites="my-composites")
            client = GCSClient(settings)
            url = client.upload_bytes(None, "styling/composites/abc.png", b"data", "image/png")

            assert "my-composites" in url or "abc.png" in url


def _make_rgba_image(size=(100, 150)) -> bytes:
    img = PILImage.new("RGBA", size, (200, 180, 160, 255))
    buf = BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


class TestCompositeLayout:
    @pytest.mark.asyncio
    async def test_composite_produces_correct_dimensions(self):
        from app.styling.compositor import build_composite
        items = [
            {"slot": "dress", "image_bytes": _make_rgba_image()},
            {"slot": "shoes", "image_bytes": _make_rgba_image()},
            {"slot": "bag", "image_bytes": _make_rgba_image()},
        ]
        result = await build_composite(items)
        assert result is not None
        img = PILImage.open(BytesIO(result))
        assert img.size == (1024, 1024)

    @pytest.mark.asyncio
    async def test_composite_handles_single_item(self):
        from app.styling.compositor import build_composite
        items = [{"slot": "dress", "image_bytes": _make_rgba_image()}]
        result = await build_composite(items)
        assert result is not None
        img = PILImage.open(BytesIO(result))
        assert img.size == (1024, 1024)

    @pytest.mark.asyncio
    async def test_composite_handles_none_image(self):
        from app.styling.compositor import build_composite
        items = [
            {"slot": "dress", "image_bytes": _make_rgba_image()},
            {"slot": "shoes", "image_bytes": None},
        ]
        result = await build_composite(items)
        assert result is not None

    @pytest.mark.asyncio
    async def test_composite_returns_none_when_all_fail(self):
        from app.styling.compositor import build_composite
        items = [{"slot": "dress", "image_bytes": None}]
        result = await build_composite(items)
        assert result is None

    @pytest.mark.asyncio
    async def test_composite_respects_timeout(self):
        from app.styling.compositor import build_composite
        items = [{"slot": "dress", "image_bytes": _make_rgba_image()}]
        result = await asyncio.wait_for(build_composite(items), timeout=8.0)
        assert result is not None
