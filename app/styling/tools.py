"""
Styling tools — deterministic functions the agent drives.
Dependencies (pg, anthropic_client, embedding_service) are injected via keyword args.
"""
import json
import logging
from typing import TYPE_CHECKING

from .schemas import OutfitBrief, ProductSummary, ProductDetail, CompatibilityReport, CandidateOutfit
from .anthropic_client import CHEAP_MODEL
from .slot_mapper import SLOT_MAP, VALID_SLOTS, subcategory_to_slot
from .palette import extract_colours, colour_overlap

if TYPE_CHECKING:
    from anthropic import AsyncAnthropic
    from app.connectors.postgres import AsyncPostgresClient
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
    max_price_gbp: float | None = None,
    k: int = 10,
    *,
    pg: "AsyncPostgresClient",
    embedding_service: "EmbeddingService",
) -> list[ProductSummary]:
    """ANN search over product index, filtered to slot and optionally price."""
    if slot not in VALID_SLOTS:
        raise ValueError(f"Unknown slot '{slot}'. Valid: {VALID_SLOTS}")

    slot_subcats = list(SLOT_MAP[slot])

    text_emb, image_emb = await embedding_service.get_embeddings(description)
    if text_emb is None and image_emb is None:
        raise RuntimeError("Embedding service returned no embeddings")

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
        results.append(ProductSummary(
            product_id=row["product_id"],
            title=row["title"] or "",
            price_gbp=float(row["price"] or 0),
            image_url=row.get("image_url") or "",
            description=row.get("description") or "",
            colour_tags=extract_colours(f"{row.get('title', '')} {row.get('description', '')}"),
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
    )
