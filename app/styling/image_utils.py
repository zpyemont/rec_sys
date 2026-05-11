import logging
from io import BytesIO

import httpx
from PIL import Image as PILImage

logger = logging.getLogger(__name__)

IMAGE_FETCH_TIMEOUT = 3.0
MAX_LONG_EDGE = 1024
_CACHE_SIZE = 500

_image_cache: dict[str, bytes] = {}
_CACHE_ORDER: list[str] = []


def _cache_put(key: str, value: bytes) -> None:
    if key in _image_cache:
        return
    if len(_CACHE_ORDER) >= _CACHE_SIZE:
        oldest = _CACHE_ORDER.pop(0)
        _image_cache.pop(oldest, None)
    _image_cache[key] = value
    _CACHE_ORDER.append(key)


def _rembg_remove(data: bytes) -> bytes:
    """Thin wrapper around rembg.remove — patched in tests to avoid llvmlite dependency."""
    from rembg import remove as _remove
    return _remove(data)


async def fetch_and_resize(url: str) -> bytes | None:
    """Fetch an image URL, resize to max 1024px long edge, return JPEG bytes. Cached."""
    if url in _image_cache:
        return _image_cache[url]
    try:
        async with httpx.AsyncClient(timeout=IMAGE_FETCH_TIMEOUT) as client:
            resp = await client.get(url)
        if resp.status_code != 200:
            logger.warning("Image fetch returned %s for %s", resp.status_code, url)
            return None
        img = PILImage.open(BytesIO(resp.content)).convert("RGB")
        w, h = img.size
        if max(w, h) > MAX_LONG_EDGE:
            scale = MAX_LONG_EDGE / max(w, h)
            img = img.resize((int(w * scale), int(h * scale)), PILImage.LANCZOS)
        buf = BytesIO()
        img.save(buf, format="JPEG", quality=85)
        result = buf.getvalue()
        _cache_put(url, result)
        return result
    except httpx.TimeoutException:
        logger.warning("Image fetch timed out for %s", url)
        return None
    except Exception as exc:
        logger.warning("Image fetch failed for %s: %s", url, exc)
        return None


def remove_background(image_bytes: bytes) -> bytes:
    """Run rembg background removal. Returns PNG bytes with alpha channel."""
    rembg_key = f"rembg:{hash(image_bytes)}"
    if rembg_key in _image_cache:
        return _image_cache[rembg_key]
    try:
        result = _rembg_remove(image_bytes)
        img = PILImage.open(BytesIO(result)).convert("RGBA")
        buf = BytesIO()
        img.save(buf, format="PNG")
        out = buf.getvalue()
        _cache_put(rembg_key, out)
        return out
    except Exception as exc:
        logger.error("rembg failed: %s — returning original", exc)
        return image_bytes
