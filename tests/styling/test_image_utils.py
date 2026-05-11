import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from io import BytesIO
from PIL import Image as PILImage


def _make_fake_image(width=800, height=600, colour=(200, 180, 160)) -> bytes:
    img = PILImage.new("RGB", (width, height), colour)
    buf = BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


def _make_fake_rgba_image(width=100, height=150) -> bytes:
    img = PILImage.new("RGBA", (width, height), (200, 180, 160, 255))
    buf = BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


class TestFetchAndResize:
    @pytest.mark.asyncio
    async def test_returns_resized_image(self):
        from app.styling.image_utils import fetch_and_resize
        fake_bytes = _make_fake_image(2000, 3000)
        with patch("app.styling.image_utils.httpx.AsyncClient") as mock_client_cls:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.content = fake_bytes
            mock_client = MagicMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.get = AsyncMock(return_value=mock_resp)
            mock_client_cls.return_value = mock_client

            result = await fetch_and_resize("https://example.com/image.jpg")
            assert result is not None
            img = PILImage.open(BytesIO(result))
            assert max(img.size) <= 1024

    @pytest.mark.asyncio
    async def test_returns_none_on_timeout(self):
        from app.styling.image_utils import fetch_and_resize
        import httpx
        with patch("app.styling.image_utils.httpx.AsyncClient") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.get = AsyncMock(side_effect=httpx.TimeoutException("timeout"))
            mock_client_cls.return_value = mock_client
            result = await fetch_and_resize("https://example.com/timeout.jpg")
            assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_404(self):
        from app.styling.image_utils import fetch_and_resize
        with patch("app.styling.image_utils.httpx.AsyncClient") as mock_client_cls:
            mock_resp = MagicMock()
            mock_resp.status_code = 404
            mock_client = MagicMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.get = AsyncMock(return_value=mock_resp)
            mock_client_cls.return_value = mock_client
            result = await fetch_and_resize("https://example.com/missing.jpg")
            assert result is None

    @pytest.mark.asyncio
    async def test_cache_is_used_on_second_call(self):
        from app.styling.image_utils import fetch_and_resize, _image_cache
        _image_cache.clear()
        fake_bytes = _make_fake_image()
        with patch("app.styling.image_utils.httpx.AsyncClient") as mock_client_cls:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.content = fake_bytes
            mock_client = MagicMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.get = AsyncMock(return_value=mock_resp)
            mock_client_cls.return_value = mock_client

            url = "https://example.com/cached-unique.jpg"
            await fetch_and_resize(url)
            await fetch_and_resize(url)
            assert mock_client.get.call_count == 1


class TestRemoveBg:
    def test_remove_bg_returns_rgba(self):
        """rembg should return an RGBA image. Mocked to avoid llvmlite/Python 3.13 incompatibility."""
        from app.styling.image_utils import remove_background
        fake_input = _make_fake_image(100, 100)
        fake_output = _make_fake_rgba_image(100, 100)

        with patch("app.styling.image_utils._rembg_remove", return_value=fake_output):
            result = remove_background(fake_input)
        img = PILImage.open(BytesIO(result))
        assert img.mode == "RGBA"

    def test_remove_bg_same_dimensions(self):
        from app.styling.image_utils import remove_background
        fake_input = _make_fake_image(100, 150)
        fake_output = _make_fake_rgba_image(100, 150)

        with patch("app.styling.image_utils._rembg_remove", return_value=fake_output):
            result = remove_background(fake_input)
        img = PILImage.open(BytesIO(result))
        assert img.size == (100, 150)
