from pydantic import BaseModel
from typing import List, Optional


class ProductItem(BaseModel):
    prod_id: str
    image_url: Optional[str] = None
    price: Optional[float] = None
    title: Optional[str] = None


class FeedResponse(BaseModel):
    feed: List[ProductItem]
