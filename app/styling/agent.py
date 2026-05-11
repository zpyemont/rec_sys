"""
Agentic styling loop.

Claude Sonnet 4.6 drives the styling tools in a loop until `finalise` is called
or the 25-call hard cap is hit. Yields SSE event dicts.

Image bytes are stripped from the conversation history after each tool dispatch
to prevent context accumulation — inspect_product and render_outfit_preview
return images for one turn only.
"""
import base64
import hashlib
import json
import logging
import time
import uuid
from typing import AsyncIterator, TYPE_CHECKING

from .schemas import (
    StyleRequest, StyleResponse, CandidateOutfit, OutfitItem, TraceStep,
)
from .tools import (
    generate_brief, search_products, inspect_product,
    check_compatibility, render_outfit_preview,
)
from .anthropic_client import AGENT_MODEL

if TYPE_CHECKING:
    from anthropic import AsyncAnthropic
    from app.connectors.postgres import AsyncPostgresClient
    from app.connectors.gcs import GCSClient
    from app.ranker.search import EmbeddingService

logger = logging.getLogger(__name__)

HARD_CAP = 25
SOFT_WARN = 20
BRIEF_CACHE_TTL = 86400  # 24h

_TOOL_DEFS = [
    {
        "name": "generate_brief",
        "description": "Expand the freeform prompt into structured aesthetic intent. Call this first to clarify palette, formality, and which outfit slots are needed.",
        "input_schema": {
            "type": "object",
            "properties": {
                "prompt": {"type": "string", "description": "The original user prompt"},
            },
            "required": ["prompt"],
        },
    },
    {
        "name": "search_products",
        "description": "Search the product index for items matching a slot and description. Returns compact candidates — call inspect_product for full details on promising hits.",
        "input_schema": {
            "type": "object",
            "properties": {
                "slot": {
                    "type": "string",
                    "enum": ["top", "bottom", "dress", "outerwear", "shoes", "bag", "accessory", "activewear"],
                },
                "description": {"type": "string", "description": "Natural language description of what you're looking for"},
                "reference_product_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional: product IDs of committed hero pieces — anchors the search visually on their images",
                },
                "max_price_gbp": {"type": "number", "description": "Optional price ceiling in GBP"},
                "k": {"type": "integer", "default": 10, "description": "Number of candidates to return"},
            },
            "required": ["slot", "description"],
        },
    },
    {
        "name": "inspect_product",
        "description": "Get full details and the actual product photo for a single product before committing to it.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_id": {"type": "string"},
            },
            "required": ["product_id"],
        },
    },
    {
        "name": "render_outfit_preview",
        "description": "Render a composite flat-lay image of multiple outfit items. Call this before check_compatibility — the compatibility tool uses this image to judge the outfit holistically.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "IDs of all items in the candidate outfit",
                },
            },
            "required": ["product_ids"],
        },
    },
    {
        "name": "check_compatibility",
        "description": "Evaluate whether 2+ products work as an outfit (palette, style, price). Call after render_outfit_preview, passing the preview_image_url.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "IDs of products to check",
                },
                "preview_image_url": {
                    "type": "string",
                    "description": "image_url returned by render_outfit_preview",
                },
            },
            "required": ["product_ids"],
        },
    },
    {
        "name": "finalise",
        "description": "Submit your completed outfits. Call this when satisfied with all outfits and compatibility has been checked. This ends the session.",
        "input_schema": {
            "type": "object",
            "properties": {
                "outfits": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "outfit_id": {"type": "string"},
                            "items": {"type": "array"},
                            "rationale": {"type": "string"},
                            "total_price_gbp": {"type": "number"},
                        },
                        "required": ["outfit_id", "items", "rationale", "total_price_gbp"],
                    },
                },
            },
            "required": ["outfits"],
        },
    },
]

_SYSTEM_PROMPT = """
You are a fashion stylist with strong taste and a real product index at your disposal.
Your job is to build {num_outfits} coherent, distinct outfits from the available products.

You can see images. When you inspect a product, the actual product photo is in the
tool result — use it. Do not rely on text descriptions alone for taste judgements.

Once you have committed to a hero piece, pass its product_id as reference_product_ids when
searching for other slots. The retrieval will use the actual image of your hero piece to find
visually compatible items — this works much better than re-describing it in words.

Before finalising any outfit, always follow this exact two-step sequence:
  1. Call render_outfit_preview with the product IDs. You will see the flat-lay composite.
     Assess it yourself: does it read as an outfit? If not, revise before the next step.
  2. Call check_compatibility, passing the preview_image_url from step 1.
     The critic will judge the composite holistically. If the score is low, revise.

Do not call check_compatibility without calling render_outfit_preview first.
Do not finalise an outfit you have not visually reviewed as a composite.

Guidelines:
- Anchor each outfit on a hero piece first (usually the slot most specified in the brief),
  then condition subsequent picks on it
- Make outfits meaningfully different from each other (not three versions of the same dress)
- Budget is a soft constraint; up to 10% over is fine, 50% over is not
- When inventory doesn't have what the brief wants, adapt and say so in the rationale
- Explicitly reconsider earlier picks when later items don't fit
- Be decisive — don't search the same slot more than twice without a good reason

Budget: {budget_constraint}
Must include categories: {must_include}
Avoid categories: {exclude}
""".strip()

# British English, present continuous tense, no emoji, no exclamation marks.
# Tone: calm and considered, like watching a stylist think.
_HUMAN_STRINGS: dict[str, str] = {
    "generate_brief": "Reading the vibe...",
    "search_products": "Searching for options...",
    "inspect_product": "Having a closer look...",
    "check_compatibility": "Checking how it reads together...",
    "render_outfit_preview": "Laying it all out...",
    "finalise": "Pulling it together...",
}


def _human_for_tool_start(tool_name: str, args: dict) -> str:
    base = _HUMAN_STRINGS.get(tool_name, f"Working on {tool_name}...")
    if tool_name == "search_products":
        slot = args.get("slot", "")
        return f"Searching for {slot}..." if slot else base
    return base


def _human_for_tool_end(tool_name: str, result_summary: str) -> str:
    if tool_name == "search_products":
        return result_summary or "Found a few options."
    if tool_name == "inspect_product":
        return result_summary or "Had a look at the details."
    if tool_name == "check_compatibility":
        return result_summary or "Assessed the outfit."
    if tool_name == "generate_brief":
        return result_summary or "Got a sense of the direction."
    if tool_name == "render_outfit_preview":
        return "Laid out the outfit."
    return result_summary or "Done."


def _strip_images_from_history(messages: list[dict]) -> None:
    """Remove image content blocks from tool_result entries in message history.

    Called after dispatching each tool batch to prevent image bytes accumulating
    in the context window. Each image is seen by the model once, then discarded.
    """
    for msg in messages:
        if msg.get("role") != "user":
            continue
        content = msg.get("content", [])
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict) or block.get("type") != "tool_result":
                continue
            result_content = block.get("content", [])
            if not isinstance(result_content, list):
                continue
            block["content"] = [
                cb for cb in result_content
                if not (isinstance(cb, dict) and cb.get("type") == "image")
            ]


async def run_styling_agent(
    *,
    request: StyleRequest,
    pg: "AsyncPostgresClient",
    anthropic_client: "AsyncAnthropic",
    embedding_service: "EmbeddingService",
    redis,
    gcs: "GCSClient | None" = None,
) -> AsyncIterator[dict]:
    """Run the agentic styling loop. Yields SSE event dicts."""
    request_id = str(uuid.uuid4())
    start_ms = time.time() * 1000
    trace: list[TraceStep] = []
    tool_call_count = 0
    final_outfits: list[CandidateOutfit] = []
    brief_interpretation = ""
    # Maps frozenset of product_ids → composite preview URL, populated by render_outfit_preview
    _preview_url_cache: dict[frozenset, str] = {}

    yield {"type": "status", "phase": "reading_vibe", "message": "Reading the vibe...", "request_id": request_id}

    budget_constraint = f"Total budget: £{request.budget_total_gbp}" if request.budget_total_gbp else "No strict budget"
    system = _SYSTEM_PROMPT.format(
        num_outfits=request.num_outfits,
        budget_constraint=budget_constraint,
        must_include=", ".join(request.must_include_categories) or "none",
        exclude=", ".join(request.exclude_categories) or "none",
    )

    messages: list[dict] = [{"role": "user", "content": request.prompt}]
    done = False

    try:
        while not done and tool_call_count < HARD_CAP:
            if tool_call_count >= SOFT_WARN:
                logger.warning(
                    "request_id=%s tool_calls=%d — agent may be thrashing",
                    request_id, tool_call_count,
                )

            response = await anthropic_client.messages.create(
                model=AGENT_MODEL,
                max_tokens=2048,
                system=system,
                tools=_TOOL_DEFS,
                messages=messages,
            )

            tool_use_blocks = [b for b in response.content if b.type == "tool_use"]

            if not tool_use_blocks:
                done = True
                break

            messages.append({"role": "assistant", "content": response.content})

            tool_results = []
            for block in tool_use_blocks:
                if tool_call_count >= HARD_CAP:
                    break

                tool_name = block.name
                args = block.input
                tool_call_count += 1
                step_num = tool_call_count

                yield {
                    "type": "tool_call_start",
                    "step": step_num,
                    "tool": tool_name,
                    "args": args,
                    "human": _human_for_tool_start(tool_name, args),
                }

                result_content: str | list = ""
                result_summary = ""

                try:
                    if tool_name == "finalise":
                        raw_outfits = args.get("outfits", [])
                        for raw in raw_outfits:
                            items = [OutfitItem(**item) for item in raw.get("items", [])]
                            item_ids = frozenset(i.product_id for i in items)
                            preview_url = _preview_url_cache.get(item_ids)
                            outfit = CandidateOutfit(
                                outfit_id=raw.get("outfit_id", str(uuid.uuid4())),
                                items=items,
                                rationale=raw.get("rationale", ""),
                                total_price_gbp=float(raw.get("total_price_gbp", 0)),
                                preview_image_url=preview_url,
                            )
                            final_outfits.append(outfit)
                            yield {
                                "type": "outfit_candidate",
                                "outfit_id": outfit.outfit_id,
                                "items": [i.model_dump() for i in outfit.items],
                                "rationale": outfit.rationale,
                                "preview_image_url": preview_url,
                            }
                        result_content = "Outfits submitted."
                        result_summary = f"Submitted {len(raw_outfits)} outfit(s)."
                        done = True

                    elif tool_name == "generate_brief":
                        cache_key = f"brief:{hashlib.sha256(args['prompt'].encode()).hexdigest()}"
                        cached = await redis.get(cache_key)
                        if cached:
                            brief_data = json.loads(cached)
                            brief_interpretation = brief_data.get("interpretation", "")
                            result_summary = f"Brief (cached): {brief_interpretation}"
                            result_content = json.dumps(brief_data)
                        else:
                            brief = await generate_brief(args["prompt"], anthropic_client=anthropic_client)
                            await redis.setex(cache_key, BRIEF_CACHE_TTL, brief.model_dump_json())
                            brief_interpretation = brief.interpretation
                            result_summary = f"Brief: {brief.interpretation}"
                            result_content = brief.model_dump_json()
                            yield {
                                "type": "status",
                                "phase": "brief_done",
                                "message": brief.interpretation,
                                "brief": brief.model_dump(),
                            }

                    elif tool_name == "search_products":
                        products = await search_products(
                            slot=args["slot"],
                            description=args["description"],
                            reference_product_ids=args.get("reference_product_ids"),
                            max_price_gbp=args.get("max_price_gbp"),
                            k=args.get("k", 10),
                            pg=pg,
                            embedding_service=embedding_service,
                        )
                        result_summary = f"Found {len(products)} {args['slot']} candidates"
                        result_content = json.dumps([p.model_dump() for p in products])

                    elif tool_name == "inspect_product":
                        detail = await inspect_product(args["product_id"], pg=pg)
                        result_summary = f"Inspected: {detail.title}"
                        text_block = {"type": "text", "text": detail.model_dump_json(exclude={"image_bytes"})}
                        if detail.image_bytes and detail.image_available:
                            result_content = [
                                {
                                    "type": "image",
                                    "source": {
                                        "type": "base64",
                                        "media_type": "image/jpeg",
                                        "data": base64.b64encode(detail.image_bytes).decode(),
                                    },
                                },
                                text_block,
                            ]
                        else:
                            result_content = text_block["text"]

                    elif tool_name == "render_outfit_preview":
                        if gcs is not None:
                            preview = await render_outfit_preview(
                                args["product_ids"],
                                pg=pg,
                                gcs=gcs,
                                redis=redis,
                            )
                            result_summary = f"Rendered composite — {'partial' if preview.partial else 'complete'}"
                            tool_result_content: list = [{"type": "text", "text": json.dumps({
                                "image_url": preview.image_url,
                                "partial": preview.partial,
                                "layout_notes": preview.layout_notes,
                            })}]
                            if not preview.partial and preview.image_bytes:
                                tool_result_content.insert(0, {
                                    "type": "image",
                                    "source": {
                                        "type": "base64",
                                        "media_type": "image/png",
                                        "data": base64.b64encode(preview.image_bytes).decode(),
                                    },
                                })
                            result_content = tool_result_content
                            if preview.image_url and not preview.partial:
                                _preview_url_cache[frozenset(args["product_ids"])] = preview.image_url
                        else:
                            result_content = json.dumps({"image_url": None, "partial": True, "layout_notes": "gcs not configured"})
                            result_summary = "render_outfit_preview skipped (gcs not configured)"

                    elif tool_name == "check_compatibility":
                        report = await check_compatibility(
                            args["product_ids"],
                            pg=pg,
                            anthropic_client=anthropic_client,
                            gcs=gcs,
                            redis=redis,
                            preview_image_url=args.get("preview_image_url"),
                        )
                        result_summary = f"Compatibility: {report.score:.2f} — {report.rationale[:80]}"
                        result_content = report.model_dump_json()

                    else:
                        result_content = f"Unknown tool: {tool_name}"
                        result_summary = result_content

                except Exception as exc:
                    logger.exception("Tool %s failed: %s", tool_name, exc)
                    result_content = f"Error: {exc}"
                    result_summary = f"{tool_name} failed: {exc}"

                trace.append(TraceStep(
                    step=step_num,
                    tool=tool_name,
                    args=args,
                    result_summary=result_summary,
                ))
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result_content,
                })

                yield {
                    "type": "tool_call_end",
                    "step": step_num,
                    "tool": tool_name,
                    "result_summary": result_summary,
                    "human": _human_for_tool_end(tool_name, result_summary),
                }

                if done:
                    break

            if tool_results:
                messages.append({"role": "user", "content": tool_results})
                _strip_images_from_history(messages)

    except Exception as exc:
        logger.exception("request_id=%s agent loop fatal error: %s", request_id, exc)
        yield {"type": "error", "code": "agent_error", "message": str(exc)}

    if tool_call_count >= HARD_CAP and not done:
        logger.error(
            "request_id=%s hit hard cap of %d tool calls with no finalise",
            request_id, HARD_CAP,
        )

    end_ms = time.time() * 1000
    payload = StyleResponse(
        request_id=request_id,
        interpretation=brief_interpretation or request.prompt,
        outfits=final_outfits,
        debug={
            "trace": [t.model_dump() for t in trace],
            "tool_calls": tool_call_count,
            "latency_ms": int(end_ms - start_ms),
            "cost_estimate_gbp": None,
        },
    )

    yield {"type": "final", **payload.model_dump()}
