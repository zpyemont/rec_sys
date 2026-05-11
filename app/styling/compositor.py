"""
Flat-lay composite generation for outfit preview.

Layout: deterministic slot-based positioning on a 1024×1024 warm-white canvas.
Background removal runs concurrently in a thread pool.

Tech debt: rembg at request time is expensive. Move to ingestion pipeline later.
"""
import asyncio
import logging
from io import BytesIO
from typing import Optional

from PIL import Image as PILImage

logger = logging.getLogger(__name__)

CANVAS_SIZE = 1024
BG_COLOUR = (245, 245, 240)  # warm white

# Slot layout: (x_centre_frac, y_centre_frac, height_frac)
_SLOT_LAYOUT = {
    "dress":      (0.50, 0.42, 0.55),
    "outerwear":  (0.22, 0.40, 0.45),
    "top":        (0.22, 0.43, 0.38),
    "bottom":     (0.22, 0.68, 0.38),
    "shoes":      (0.50, 0.88, 0.22),
    "bag":        (0.78, 0.45, 0.28),
    "accessory":  (0.78, 0.72, 0.20),
    "activewear": (0.50, 0.42, 0.55),
}


def _place_item(canvas: PILImage.Image, item_png: bytes, slot: str) -> None:
    layout = _SLOT_LAYOUT.get(slot)
    if layout is None:
        return
    cx_frac, cy_frac, h_frac = layout
    target_h = int(CANVAS_SIZE * h_frac)

    item_img = PILImage.open(BytesIO(item_png)).convert("RGBA")
    w, h = item_img.size
    if h > 0:
        scale = target_h / h
        target_w = max(1, int(w * scale))
        item_img = item_img.resize((target_w, target_h), PILImage.LANCZOS)

    cx = int(CANVAS_SIZE * cx_frac)
    cy = int(CANVAS_SIZE * cy_frac)
    paste_x = cx - item_img.width // 2
    paste_y = cy - item_img.height // 2
    canvas.paste(item_img, (paste_x, paste_y), item_img)


def _remove_bg_sync(image_bytes: bytes) -> Optional[bytes]:
    """Synchronous rembg call — run in thread pool."""
    try:
        from app.styling.image_utils import remove_background
        return remove_background(image_bytes)
    except Exception as exc:
        logger.warning("Background removal failed: %s", exc)
        return None


async def build_composite(items: list[dict]) -> Optional[bytes]:
    """
    Build a 1024×1024 flat-lay composite from outfit items.

    items: list of {"slot": str, "image_bytes": bytes | None}
    Returns PNG bytes, or None if all items failed.
    """
    loop = asyncio.get_event_loop()

    removal_tasks = []
    for item in items:
        if item.get("image_bytes"):
            removal_tasks.append(
                loop.run_in_executor(None, _remove_bg_sync, item["image_bytes"])
            )
        else:
            removal_tasks.append(asyncio.sleep(0, result=None))

    try:
        rembg_results = await asyncio.wait_for(
            asyncio.gather(*removal_tasks, return_exceptions=True),
            timeout=7.0,
        )
    except asyncio.TimeoutError:
        logger.warning("Background removal timed out — using partial results")
        rembg_results = [None] * len(items)

    canvas = PILImage.new("RGB", (CANVAS_SIZE, CANVAS_SIZE), BG_COLOUR)
    any_placed = False
    for item, bg_result in zip(items, rembg_results):
        if isinstance(bg_result, Exception) or bg_result is None:
            continue
        try:
            _place_item(canvas, bg_result, item["slot"])
            any_placed = True
        except Exception as exc:
            logger.warning("Failed to place item slot=%s: %s", item.get("slot"), exc)

    if not any_placed:
        return None

    buf = BytesIO()
    canvas.save(buf, format="PNG", optimize=True)
    return buf.getvalue()
