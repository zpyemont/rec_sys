from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class ProductItem(BaseModel):
    id: str  # Renamed from prod_id
    title: Optional[str] = None
    price: Optional[float] = None
    compare_at_price: Optional[float] = None
    images: Optional[List[str]] = None  # Changed from image_url (single) to images (array)
    image_has_text: Optional[List[bool]] = None  # CLIP classifier results per image
    category: Optional[str] = None
    subcategory: Optional[str] = None  # Specific subcategory for hierarchical categorization
    like_count: int = 0
    description: Optional[str] = None
    url: Optional[str] = None  # Product page URL (formerly affiliateUrl)
    brand: Optional[str] = None
    created_at: Optional[datetime] = None
    currency: Optional[str] = None
    availability: Optional[str] = None


class FeedResponse(BaseModel):
    feed: List[ProductItem]
    request_id: Optional[int] = None  # For tracking interactions in recommendation pipeline

    # Pagination metadata
    has_more: bool = True              # Are there more products available?
    unseen_count: Optional[int] = None # Products in current candidate pool
    shown_count: Optional[int] = None  # Total items user has seen
    total_count: Optional[int] = None  # Total products in catalog
    tier: Optional[int] = None         # Which tier user is in (1-4)

    # Session metadata (for debugging and analytics)
    session_id: Optional[str] = None           # Echo back session ID
    cold_start_stage: Optional[str] = None     # User's personalization stage
    session_positive_count: Optional[int] = None  # Products with positive engagement in session


# New schemas for like/unlike endpoints
class LikeRequest(BaseModel):
    user_id: str
    product_id: str


class LikeResponse(BaseModel):
    success: bool
    like_count: int
    message: Optional[str] = None


# Collection schemas
class CollectionItem(BaseModel):
    id: str
    name: str
    created_at: str  # ISO string
    updated_at: str  # ISO string
    product_count: int
    products: List[ProductItem] = []  # Products in the collection


class CollectionsResponse(BaseModel):
    collections: List[CollectionItem]


# Tracking schemas
class TrackRequest(BaseModel):
    request_id: int
    user_id: str
    session_id: Optional[str] = None  # Client-generated session ID for session-aware recommendations
    product_id: str
    action: str  # "swipe_up", "swipe_down", "like", "unlike", "collection_add", "shop_now"
    dwell_time: float
    images_viewed: int
    position: int


class TrackResponse(BaseModel):
    status: str
    request_id: int
    message: Optional[str] = None
