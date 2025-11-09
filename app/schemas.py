from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class ProductItem(BaseModel):
    id: str  # Renamed from prod_id
    title: Optional[str] = None
    price: Optional[float] = None
    images: Optional[List[str]] = None  # Changed from image_url (single) to images (array)
    category: Optional[str] = None
    like_count: int = 0
    description: Optional[str] = None
    url: Optional[str] = None  # Product page URL (formerly affiliateUrl)
    brand: Optional[str] = None
    created_at: Optional[datetime] = None
    currency: Optional[str] = None
    availability: Optional[str] = None


class FeedResponse(BaseModel):
    feed: List[ProductItem]


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
    product_id: str
    action: str  # "swipe_up", "swipe_down", "like", "collection_add", "shop_now"
    dwell_time: float
    images_viewed: int
    position: int


class TrackResponse(BaseModel):
    status: str
    request_id: int
    message: Optional[str] = None
