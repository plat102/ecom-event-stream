"""The 27 valid raw `collection` (renamed `event_type`) values — a static allow-list, not a
dimension table. An event outside this set is unknown, not just unmapped, and gets routed
to the dead-letter queue instead of `fact_event`.
"""
KNOWN_EVENT_TYPES = {
    "view_home_page", "view_landing_page", "view_listing_page",
    "view_static_page", "view_my_account", "view_sorting_relevance",
    "view_product_detail", "select_product_option", "select_product_option_quality",
    "view_shopping_cart", "add_to_cart_action",
    "checkout", "checkout_success",
    "search_box_action",
    "view_all_recommend", "product_view_all_recommend_clicked",
    "landing_page_recommendation_visible", "landing_page_recommendation_noticed",
    "landing_page_recommendation_clicked",
    "listing_page_recommendation_visible", "listing_page_recommendation_noticed",
    "listing_page_recommendation_clicked",
    "product_detail_recommendation_visible", "product_detail_recommendation_noticed",
    "product_detail_recommendation_clicked",
}
