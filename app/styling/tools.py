"""
Styling tools — deterministic functions the agent drives.
Dependencies (pg, anthropic_client, embedding_service) are injected via keyword args.
"""
import hashlib
import json
import logging
import statistics
import uuid
from typing import TYPE_CHECKING

from .schemas import OutfitBrief, ProductHit, ProductSummary, ProductDetail, CompatibilityReport, CandidateOutfit, RenderedPreview
from .anthropic_client import CHEAP_MODEL
from .slot_mapper import SLOT_MAP, VALID_SLOTS, subcategory_to_slot
from .palette import extract_colours, colour_overlap
from .image_utils import fetch_and_resize

if TYPE_CHECKING:
    from anthropic import AsyncAnthropic
    from app.connectors.postgres import AsyncPostgresClient
    from app.connectors.gcs import GCSClient
    from app.ranker.search import EmbeddingService

logger = logging.getLogger(__name__)

_BRIEF_SYSTEM = """
You are a fashion stylist assistant. Given a freeform vibe prompt, return a JSON object
describing the aesthetic intent. Return ONLY valid JSON matching this schema:
{
  "interpretation": "one sentence describing the aesthetic",
  "aesthetic_tags": ["tag1", "tag2"],
  "palette": ["colour1", "colour2"],
  "formality": "casual|smart-casual|formal|black-tie",
  "occasion": "string or null",
  "slots_needed": ["top|bottom|dress|outerwear|shoes|bag|accessory"],
  "notes": "tensions, tradeoffs, things to watch for"
}
"""


async def generate_brief(prompt: str, *, anthropic_client: "AsyncAnthropic") -> OutfitBrief:
    """Expand a freeform prompt into structured aesthetic intent."""
    response = await anthropic_client.messages.create(
        model=CHEAP_MODEL,
        max_tokens=512,
        system=_BRIEF_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = response.content[0].text.strip()
    try:
        data = json.loads(raw)
        return OutfitBrief(**data)
    except Exception as e:
        raise ValueError(f"Failed to parse brief: {e}\nRaw: {raw}") from e


async def search_products(
    slot: str,
    description: str,
    reference_product_ids: list[str] | None = None,
    max_price_gbp: float | None = None,
    k: int = 10,
    *,
    pg: "AsyncPostgresClient",
    embedding_service: "EmbeddingService",
) -> list[ProductHit]:
    """ANN search over product index, filtered to slot and optionally price.

    When reference_product_ids is provided, their stored CLIP image embeddings
    are averaged with the query's CLIP text embedding (both 512-dim, same aligned
    space) to anchor the search visually on a committed hero piece.
    """
    import numpy as np

    if slot not in VALID_SLOTS:
        raise ValueError(f"Unknown slot '{slot}'. Valid: {VALID_SLOTS}")

    slot_subcats = list(SLOT_MAP[slot])

    text_emb, image_emb = await embedding_service.get_embeddings(description)
    if text_emb is None and image_emb is None:
        raise RuntimeError("Embedding service returned no embeddings")

    # CLIP text and image embeddings share the same 512-dim aligned space —
    # averaging is valid (Option 1 from spec).
    clip_emb = image_emb  # 512-dim CLIP text-to-image embedding

    ref_averaged = False
    if reference_product_ids and clip_emb is not None:
        ref_rows = await pg.fetch_all(
            "SELECT image_embedding FROM embeddings.product_vectors "
            "WHERE product_id = ANY($1::text[]) AND image_embedding IS NOT NULL",
            [reference_product_ids],
        )
        if ref_rows:
            ref_vecs = [row["image_embedding"] for row in ref_rows]
            all_vecs = np.array([clip_emb] + ref_vecs, dtype=float)
            averaged = all_vecs.mean(axis=0)
            norm = np.linalg.norm(averaged)
            clip_emb = (averaged / norm).tolist() if norm > 0 else clip_emb
            ref_averaged = True

    # When reference images were averaged in, search CLIP image space.
    # Otherwise use the higher-quality Marqo text embedding for semantic matching.
    if ref_averaged and clip_emb is not None:
        embedding = clip_emb
        emb_col = "image_embedding"
    else:
        embedding = text_emb if text_emb is not None else image_emb
        emb_col = "text_embedding" if text_emb is not None else "image_embedding"

    emb_str = "[" + ",".join(str(x) for x in embedding) + "]"

    price_clause = "AND pp.price <= $4" if max_price_gbp is not None else ""
    params: list = [emb_str, slot_subcats, k]
    if max_price_gbp is not None:
        params.append(max_price_gbp)

    sql = f"""
        SELECT
            p.product_id,
            p.title,
            pp.price,
            pp.currency,
            pi.image_url,
            p.description,
            p.subcategory,
            p.url,
            e.{emb_col} <=> $1::vector AS distance
        FROM embeddings.product_vectors e
        JOIN catalog.products p USING (product_id)
        JOIN catalog.product_pricing pp USING (product_id)
        LEFT JOIN LATERAL (
            SELECT image_url FROM catalog.product_images
            WHERE product_id = p.product_id AND has_text_overlay IS DISTINCT FROM TRUE
            ORDER BY position LIMIT 1
        ) pi ON TRUE
        WHERE p.subcategory = ANY($2::text[])
          AND pp.is_active = TRUE
          {price_clause}
        ORDER BY distance
        LIMIT $3
    """
    rows = await pg.fetch_all(sql, params)

    results = []
    for row in rows:
        if row.get("currency") and row["currency"] != "GBP":
            logger.warning(
                "Product %s has currency %s, treating as GBP",
                row["product_id"], row["currency"],
            )
        results.append(ProductHit(
            product_id=row["product_id"],
            title=row["title"] or "",
            price_gbp=float(row["price"] or 0),
            slot=slot,
            subcategory=row.get("subcategory") or slot,
        ))
    return results


async def inspect_product(product_id: str, *, pg: "AsyncPostgresClient") -> ProductDetail:
    """Full details on a single product."""
    row = await pg.fetch_one(
        """
        SELECT p.product_id, p.title, p.description, p.subcategory, p.url,
               pp.price, pp.currency
        FROM catalog.products p
        JOIN catalog.product_pricing pp USING (product_id)
        WHERE p.product_id = $1 AND pp.is_active = TRUE
        LIMIT 1
        """,
        [product_id],
    )
    if row is None:
        raise ValueError(f"Product not found: {product_id}")

    images = await pg.fetch_all(
        "SELECT image_url FROM catalog.product_images WHERE product_id = $1 ORDER BY position",
        [product_id],
    )
    image_urls = [r["image_url"] for r in images if r.get("image_url")]
    description = row.get("description") or ""
    palette = extract_colours(f"{row.get('title', '')} {description}")
    slot = subcategory_to_slot(row.get("subcategory") or "") or "accessory"

    image_bytes = None
    image_available = False
    if image_urls:
        image_bytes = await fetch_and_resize(image_urls[0])
        image_available = image_bytes is not None
        if not image_available:
            logger.warning("Could not fetch image for product %s", product_id)

    return ProductDetail(
        product_id=row["product_id"],
        title=row["title"] or "",
        price_gbp=float(row["price"] or 0),
        images=image_urls,
        description=description,
        materials=None,
        fit_notes=None,
        palette=palette,
        affiliate_url=row.get("url") or "",
        slot=slot,
        subcategory=row.get("subcategory") or "",
        image_bytes=image_bytes,
        image_available=image_available,
    )


# ---------------------------------------------------------------------------
# Compatibility scoring helpers
# ---------------------------------------------------------------------------

def _palette_overlap_score(colours_a: list[str], colours_b: list[str]) -> float:
    return colour_overlap(colours_a, colours_b)


def _style_compatibility_score(subcat_a: str, subcat_b: str) -> float:
    """1.0 if mutual adjacency, 0.5 if one-way, 0.0 if unrelated."""
    from app.settings import get_settings
    adj = get_settings().category_adjacency
    a_adj = set(adj.get(subcat_a, []))
    b_adj = set(adj.get(subcat_b, []))
    if subcat_b in a_adj and subcat_a in b_adj:
        return 1.0
    if subcat_b in a_adj or subcat_a in b_adj:
        return 0.5
    return 0.0


def _price_coherence_score(prices: list[float]) -> float:
    """1 - CV (coefficient of variation), clamped to [0, 1]. Higher = more coherent."""
    if len(prices) <= 1:
        return 1.0
    mean = statistics.mean(prices)
    if mean == 0:
        return 1.0
    cv = statistics.stdev(prices) / mean
    return max(0.0, 1.0 - cv)


_COMPAT_SYSTEM = """
You are a fashion stylist. Given a list of clothing items, write one short sentence
explaining whether they work together as an outfit and why. Be specific about colour,
silhouette, and occasion. Return only the sentence, no preamble.
"""


async def check_compatibility(
    product_ids: list[str],
    *,
    pg: "AsyncPostgresClient",
    anthropic_client: "AsyncAnthropic",
    gcs: "GCSClient | None" = None,
    redis=None,
    preview_image_url: str | None = None,
    preview_image_bytes: bytes | None = None,
) -> CompatibilityReport:
    """Evaluate outfit coherence across palette, style, and price dimensions.

    When gcs + redis are provided, renders a composite internally if no preview
    is passed. Agent should call render_outfit_preview first and pass the result
    through; the internal render path is a slow fallback.
    """
    import base64

    if len(product_ids) < 2:
        raise ValueError("check_compatibility requires at least 2 products")

    rows = await pg.fetch_all(
        """
        SELECT p.product_id, p.title, p.description, p.subcategory, pp.price
        FROM catalog.products p
        JOIN catalog.product_pricing pp USING (product_id)
        WHERE p.product_id = ANY($1::text[]) AND pp.is_active = TRUE
        """,
        [product_ids],
    )

    colours_per_item = [
        extract_colours(f"{r.get('title', '')} {r.get('description', '')}") for r in rows
    ]

    palette_scores = []
    for i in range(len(colours_per_item)):
        for j in range(i + 1, len(colours_per_item)):
            palette_scores.append(colour_overlap(colours_per_item[i], colours_per_item[j]))
    palette_overlap_val = statistics.mean(palette_scores) if palette_scores else 0.0

    subcats = [r.get("subcategory") or "" for r in rows]
    style_scores = []
    for i in range(len(subcats)):
        for j in range(i + 1, len(subcats)):
            style_scores.append(_style_compatibility_score(subcats[i], subcats[j]))
    style_compat = statistics.mean(style_scores) if style_scores else 0.0

    prices = [float(r.get("price") or 0) for r in rows]
    price_coh = _price_coherence_score(prices)

    score = 0.4 * palette_overlap_val + 0.35 * style_compat + 0.25 * price_coh

    # Render composite if gcs/redis available and no preview passed in.
    # If preview_image_url was already provided (agent called render_outfit_preview
    # first), skip re-render and mark as not partial so the URL is included in response.
    preview_partial = True
    if preview_image_url is not None:
        preview_partial = False
    elif preview_image_bytes is None and gcs is not None and redis is not None:
        logger.warning("check_compatibility called without preview — rendering internally (slow path)")
        preview = await render_outfit_preview(product_ids, pg=pg, gcs=gcs, redis=redis)
        preview_image_url = preview.image_url or None
        preview_image_bytes = preview.image_bytes if not preview.partial else None
        preview_partial = preview.partial
    elif preview_image_bytes is not None:
        preview_partial = len(preview_image_bytes) == 0

    item_descriptions = "\n".join(
        f"- {r.get('title', '?')} ({r.get('subcategory', '?')}, £{r.get('price', 0):.0f})"
        for r in rows
    )
    text_block = {
        "type": "text",
        "text": (
            f"Outfit items:\n{item_descriptions}\n\n"
            f"Palette overlap: {palette_overlap_val:.2f}, "
            f"Style compatibility: {style_compat:.2f}, "
            f"Price coherence: {price_coh:.2f}"
        ),
    }
    content_blocks: list = [text_block]
    if preview_image_bytes and not preview_partial:
        content_blocks.insert(0, {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/png",
                "data": base64.b64encode(preview_image_bytes).decode(),
            },
        })

    msg = await anthropic_client.messages.create(
        model=CHEAP_MODEL,
        max_tokens=150,
        system=_COMPAT_SYSTEM,
        messages=[{"role": "user", "content": content_blocks}],
    )
    rationale = msg.content[0].text.strip()

    return CompatibilityReport(
        score=round(score, 3),
        palette_overlap=round(palette_overlap_val, 3),
        style_compatibility=round(style_compat, 3),
        price_coherence=round(price_coh, 3),
        rationale=rationale,
        compatible=score >= 0.5,
        preview_image_url=preview_image_url if not preview_partial else None,
    )


COMPOSITE_CACHE_TTL = 3600  # 1 hour


async def render_outfit_preview(
    product_ids: list[str],
    *,
    pg: "AsyncPostgresClient",
    gcs: "GCSClient",
    redis,
    cached_image_bytes: bytes | None = None,
) -> RenderedPreview:
    """Render a flat-lay composite of outfit items. Caches by product set in Redis."""
    from .compositor import build_composite

    cache_key = f"composite:{hashlib.sha256(','.join(sorted(product_ids)).encode()).hexdigest()}"
    cached = await redis.get(cache_key)
    if cached and cached_image_bytes is not None:
        data = json.loads(cached)
        return RenderedPreview(
            image_bytes=cached_image_bytes,
            image_url=data["image_url"],
            layout_notes=data.get("layout_notes", ""),
            partial=data.get("partial", False),
        )

    # Fetch primary image URL and subcategory for each product
    image_rows = await pg.fetch_all(
        """
        SELECT p.product_id, pi.image_url
        FROM catalog.products p
        LEFT JOIN LATERAL (
            SELECT image_url FROM catalog.product_images
            WHERE product_id = p.product_id AND has_text_overlay IS DISTINCT FROM TRUE
            ORDER BY position LIMIT 1
        ) pi ON TRUE
        WHERE p.product_id = ANY($1::text[])
        """,
        [product_ids],
    )
    url_by_id = {r["product_id"]: r.get("image_url") for r in image_rows}

    slot_rows = await pg.fetch_all(
        "SELECT product_id, subcategory FROM catalog.products WHERE product_id = ANY($1::text[])",
        [product_ids],
    )
    slot_by_id = {
        r["product_id"]: subcategory_to_slot(r.get("subcategory") or "") or "accessory"
        for r in slot_rows
    }

    items_for_composite = []
    for pid in product_ids:
        url = url_by_id.get(pid)
        img_bytes = await fetch_and_resize(url) if url else None
        items_for_composite.append({"slot": slot_by_id.get(pid, "accessory"), "image_bytes": img_bytes})

    composite_bytes = await build_composite(items_for_composite)
    partial = composite_bytes is None

    image_url = ""
    if composite_bytes:
        blob_name = f"styling/composites/{uuid.uuid4()}.png"
        image_url = gcs.upload_bytes(None, blob_name, composite_bytes, "image/png")

    await redis.setex(
        cache_key,
        COMPOSITE_CACHE_TTL,
        json.dumps({"image_url": image_url, "layout_notes": "slot-based flat-lay", "partial": partial}),
    )

    return RenderedPreview(
        image_bytes=composite_bytes or b"",
        image_url=image_url,
        layout_notes="slot-based flat-lay, v1",
        partial=partial,
    )


def finalise(outfits: list[CandidateOutfit]) -> None:
    """Terminal action — signals the agent loop to stop."""
    return None
