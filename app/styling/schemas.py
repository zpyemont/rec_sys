from typing import Literal
from pydantic import BaseModel


class OutfitBrief(BaseModel):
    interpretation: str
    aesthetic_tags: list[str]
    palette: list[str]
    formality: Literal["casual", "smart-casual", "formal", "black-tie"]
    occasion: str | None
    slots_needed: list[str]
    notes: str


class ProductSummary(BaseModel):
    product_id: str
    title: str
    price_gbp: float
    image_url: str
    description: str
    colour_tags: list[str]
    slot: str
    subcategory: str


class ProductDetail(BaseModel):
    product_id: str
    title: str
    price_gbp: float
    images: list[str]
    description: str
    materials: str | None
    fit_notes: str | None
    palette: list[str]
    affiliate_url: str
    slot: str
    subcategory: str
    image_bytes: bytes | None = None
    image_available: bool = True


class CompatibilityReport(BaseModel):
    score: float
    palette_overlap: float
    style_compatibility: float
    price_coherence: float
    rationale: str
    compatible: bool
    preview_image_url: str | None = None


class OutfitItem(BaseModel):
    slot: str
    product_id: str
    title: str
    price_gbp: float
    image_url: str
    affiliate_url: str
    match_reason: str


class CandidateOutfit(BaseModel):
    outfit_id: str
    items: list[OutfitItem]
    rationale: str
    total_price_gbp: float


class TraceStep(BaseModel):
    step: int
    tool: str
    args: dict
    result_summary: str


class StyleRequest(BaseModel):
    prompt: str
    num_outfits: int = 3
    budget_total_gbp: float | None = None
    must_include_categories: list[str] = []
    exclude_categories: list[str] = []
    stream: bool = False


class StyleResponse(BaseModel):
    request_id: str
    interpretation: str
    outfits: list[CandidateOutfit]
    debug: dict


class SwapRequest(BaseModel):
    outfit: CandidateOutfit
    slot_to_swap: str
    original_prompt: str


class SwapResponse(BaseModel):
    new_item: OutfitItem
    total_price_gbp: float


class RenderedPreview(BaseModel):
    image_bytes: bytes
    image_url: str
    layout_notes: str
    partial: bool = False
