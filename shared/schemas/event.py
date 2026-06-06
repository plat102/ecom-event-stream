from typing import Any, Optional
from pydantic import BaseModel, Field

# product_id excluded: absent in browsing/recommendation events
REQUIRED_FIELDS = {"id", "collection", "local_time"}


class RawEvent(BaseModel):
    # Always-present fields
    id: str
    collection: str
    local_time: str

    # System
    source_id: Optional[str] = Field(None, alias="_id")
    api_version: Optional[str] = None
    collect_id: Optional[str] = None
    time_stamp: Optional[int] = None

    # Navigation
    current_url: Optional[str] = None
    referrer_url: Optional[str] = None

    # Identity
    device_id: Optional[str] = None
    email_address: Optional[str] = None
    ip: Optional[str] = None
    user_id_db: Optional[str] = None

    # Context
    resolution: Optional[str] = None
    store_id: Optional[str] = None
    user_agent: Optional[str] = None
    utm_medium: Optional[Any] = None  # bool or str
    utm_source: Optional[Any] = None  # bool or str

    # Product (absent in browsing/recommendation events)
    cat_id: Optional[Any] = None
    currency: Optional[str] = None
    key_search: Optional[Any] = None
    option: Optional[Any] = None  # list[dict] or dict depending on event type
    price: Optional[str] = None
    product_id: Optional[str] = None
    viewing_product_id: Optional[str] = None

    # Cart / checkout
    cart_products: Optional[list] = None
    is_paypal: Optional[Any] = None
    order_id: Optional[Any] = None  # string, int, or float depending on event

    # Recommendation
    recommendation: Optional[bool] = None
    recommendation_clicked_position: Optional[Any] = None
    recommendation_product_id: Optional[str] = None
    recommendation_product_position: Optional[Any] = None  # int or string
    show_recommendation: Optional[str] = None
