-- Reporting layer as plain views — a changed requirement is one edited SELECT, no backfill.
--   Layer 2 (v_*_enriched): one per event family, joins the dimensions once.
--   Layer 3: the aggregates the dashboard reads.
-- Report views expose report_date instead of hard-coding CURRENT_DATE, so the caller filters.
-- Separate file from create_tables.sql: rebuilding fact_event drops these, this puts them back.

DROP VIEW IF EXISTS
    v_top_products_today, v_top_countries_today, v_top_referrers_today,
    v_stores_by_country, v_product_hourly, v_device_hourly,
    v_product_view_enriched, v_cart_enriched, v_checkout_enriched,
    v_search_enriched, v_recommendation_enriched, v_browse_enriched
CASCADE;

-- ═══════════════════════════════════════════════════════════════════════
-- Layer 2 — analytical views, one per event family
-- ═══════════════════════════════════════════════════════════════════════

-- Product detail views — the base for all six reports below.
CREATE VIEW v_product_view_enriched AS
SELECT
    f.event_key,
    f.event_id,
    f.event_type,
    -- Time
    f.report_date, f.hour, f.event_timestamp,
    dd.day_of_week, dd.is_weekend, dd.month, dd.year, dd.week_of_year,
    -- Product
    f.product_key,
    dp.product_id, dp.product_name, dp.category_name,
    -- Geography the user chose (site locale, from the URL)
    f.site_key,
    ds.country_domain, ds.country_name AS url_country_name, ds.continent, ds.timezone,
    f.store_id,
    -- Geography the user is actually in (from the IP)
    f.location_key,
    dl.country_name AS ip_country_name, dl.region_name AS ip_region,
    dl.city_name AS ip_city, dl.has_geo_data,
    -- Three-valued on purpose: NULL when either side is unknown. An AND-chain would say
    -- FALSE there, because IS NOT NULL never yields NULL.
    CASE WHEN dl.country_name IS NULL OR ds.country_name IS NULL THEN NULL
         ELSE dl.country_name <> ds.country_name END AS is_cross_border,
    -- Traffic source, raw — there is no channel grouping dimension.
    f.utm_source, f.utm_medium, f.referrer_url,
    -- Tech
    f.device_key,
    dv.browser, dv.os, dv.device_category, dv.is_mobile,
    -- Identity
    f.device_id, f.user_id_db, f.email_hash, f.session_id,
    -- Event-specific
    f.payload->'option' AS product_options
FROM fact_event f
JOIN      dim_date     dd ON f.date_key     = dd.date_key
LEFT JOIN dim_site     ds ON f.site_key     = ds.site_key
LEFT JOIN dim_location dl ON f.location_key = dl.location_key
LEFT JOIN dim_product  dp ON f.product_key  = dp.product_key
LEFT JOIN dim_device   dv ON f.device_key   = dv.device_key
WHERE f.event_type = 'view_product_detail';

-- Cart activity.
CREATE VIEW v_cart_enriched AS
SELECT
    f.event_key, f.event_id, f.event_type,
    f.report_date, f.hour, f.event_timestamp,
    dd.day_of_week, dd.is_weekend, dd.month, dd.year, dd.week_of_year,
    f.product_key, dp.product_id, dp.product_name, dp.category_name,
    f.site_key, ds.country_domain, ds.country_name AS url_country_name, ds.continent,
    f.store_id,
    f.location_key,
    dl.country_name AS ip_country_name, dl.region_name AS ip_region,
    dl.city_name AS ip_city, dl.has_geo_data,
    CASE WHEN dl.country_name IS NULL OR ds.country_name IS NULL THEN NULL
         ELSE dl.country_name <> ds.country_name END AS is_cross_border,
    f.utm_source, f.utm_medium, f.referrer_url,
    f.device_key, dv.browser, dv.os, dv.device_category, dv.is_mobile,
    f.device_id, f.user_id_db, f.email_hash, f.session_id,
    f.cart_item_count,
    f.payload->'cart_products'   AS cart_products,
    f.payload->>'is_paypal'      AS is_paypal
FROM fact_event f
JOIN      dim_date     dd ON f.date_key     = dd.date_key
LEFT JOIN dim_site     ds ON f.site_key     = ds.site_key
LEFT JOIN dim_location dl ON f.location_key = dl.location_key
LEFT JOIN dim_product  dp ON f.product_key  = dp.product_key
LEFT JOIN dim_device   dv ON f.device_key   = dv.device_key
WHERE f.event_type IN ('view_shopping_cart', 'add_to_cart_action');

-- Checkout funnel. checkout_success is the only conversion event.
CREATE VIEW v_checkout_enriched AS
SELECT
    f.event_key, f.event_id, f.event_type,
    f.report_date, f.hour, f.event_timestamp,
    dd.day_of_week, dd.is_weekend, dd.month, dd.year, dd.week_of_year,
    f.product_key, dp.product_id, dp.product_name, dp.category_name,
    f.site_key, ds.country_domain, ds.country_name AS url_country_name, ds.continent,
    f.store_id,
    f.location_key,
    dl.country_name AS ip_country_name, dl.region_name AS ip_region,
    dl.city_name AS ip_city, dl.has_geo_data,
    CASE WHEN dl.country_name IS NULL OR ds.country_name IS NULL THEN NULL
         ELSE dl.country_name <> ds.country_name END AS is_cross_border,
    f.utm_source, f.utm_medium, f.referrer_url,
    f.device_key, dv.browser, dv.os, dv.device_category, dv.is_mobile,
    f.device_id, f.user_id_db, f.email_hash, f.session_id,
    f.order_id, f.cart_item_count,
    f.payload->'cart_products' AS cart_products,
    (f.event_type = 'checkout_success') AS is_conversion
FROM fact_event f
JOIN      dim_date     dd ON f.date_key     = dd.date_key
LEFT JOIN dim_site     ds ON f.site_key     = ds.site_key
LEFT JOIN dim_location dl ON f.location_key = dl.location_key
LEFT JOIN dim_product  dp ON f.product_key  = dp.product_key
LEFT JOIN dim_device   dv ON f.device_key   = dv.device_key
WHERE f.event_type IN ('checkout', 'checkout_success');

-- Site search.
CREATE VIEW v_search_enriched AS
SELECT
    f.event_key, f.event_id, f.event_type,
    f.report_date, f.hour, f.event_timestamp,
    dd.day_of_week, dd.is_weekend, dd.month, dd.year, dd.week_of_year,
    f.site_key, ds.country_domain, ds.country_name AS url_country_name, ds.continent,
    f.store_id,
    f.location_key,
    dl.country_name AS ip_country_name, dl.region_name AS ip_region,
    dl.city_name AS ip_city, dl.has_geo_data,
    CASE WHEN dl.country_name IS NULL OR ds.country_name IS NULL THEN NULL
         ELSE dl.country_name <> ds.country_name END AS is_cross_border,
    f.utm_source, f.utm_medium, f.referrer_url,
    f.device_key, dv.browser, dv.os, dv.device_category, dv.is_mobile,
    f.device_id, f.user_id_db, f.email_hash, f.session_id,
    f.key_search
FROM fact_event f
JOIN      dim_date     dd ON f.date_key     = dd.date_key
LEFT JOIN dim_site     ds ON f.site_key     = ds.site_key
LEFT JOIN dim_location dl ON f.location_key = dl.location_key
LEFT JOIN dim_device   dv ON f.device_key   = dv.device_key
WHERE f.event_type = 'search_box_action';

-- Recommendation widgets: shown, scrolled into view, clicked, across three page types.
CREATE VIEW v_recommendation_enriched AS
SELECT
    f.event_key, f.event_id, f.event_type,
    f.report_date, f.hour, f.event_timestamp,
    dd.day_of_week, dd.is_weekend, dd.month, dd.year, dd.week_of_year,
    f.product_key, dp.product_id, dp.product_name, dp.category_name,
    f.site_key, ds.country_domain, ds.country_name AS url_country_name, ds.continent,
    f.store_id,
    f.location_key,
    dl.country_name AS ip_country_name, dl.region_name AS ip_region,
    dl.city_name AS ip_city, dl.has_geo_data,
    CASE WHEN dl.country_name IS NULL OR ds.country_name IS NULL THEN NULL
         ELSE dl.country_name <> ds.country_name END AS is_cross_border,
    f.utm_source, f.utm_medium, f.referrer_url,
    f.device_key, dv.browser, dv.os, dv.device_category, dv.is_mobile,
    f.device_id, f.user_id_db, f.email_hash, f.session_id,
    f.event_type AS recommendation_action,
    f.recommendation_position,
    f.payload->>'recommendation_product_id' AS recommendation_product_id
FROM fact_event f
JOIN      dim_date     dd ON f.date_key     = dd.date_key
LEFT JOIN dim_site     ds ON f.site_key     = ds.site_key
LEFT JOIN dim_location dl ON f.location_key = dl.location_key
LEFT JOIN dim_product  dp ON f.product_key  = dp.product_key
LEFT JOIN dim_device   dv ON f.device_key   = dv.device_key
WHERE f.event_type IN (
    'view_all_recommend', 'product_view_all_recommend_clicked',
    'landing_page_recommendation_visible', 'landing_page_recommendation_noticed',
    'landing_page_recommendation_clicked',
    'listing_page_recommendation_visible', 'listing_page_recommendation_noticed',
    'listing_page_recommendation_clicked',
    'product_detail_recommendation_visible', 'product_detail_recommendation_noticed',
    'product_detail_recommendation_clicked'
);

-- Page views that aren't the product detail page.
CREATE VIEW v_browse_enriched AS
SELECT
    f.event_key, f.event_id, f.event_type,
    f.report_date, f.hour, f.event_timestamp,
    dd.day_of_week, dd.is_weekend, dd.month, dd.year, dd.week_of_year,
    f.site_key, ds.country_domain, ds.country_name AS url_country_name, ds.continent,
    f.store_id,
    f.location_key,
    dl.country_name AS ip_country_name, dl.region_name AS ip_region,
    dl.city_name AS ip_city, dl.has_geo_data,
    CASE WHEN dl.country_name IS NULL OR ds.country_name IS NULL THEN NULL
         ELSE dl.country_name <> ds.country_name END AS is_cross_border,
    f.utm_source, f.utm_medium, f.referrer_url,
    f.device_key, dv.browser, dv.os, dv.device_category, dv.is_mobile,
    f.device_id, f.user_id_db, f.email_hash, f.session_id,
    f.event_type AS page_type,
    f.current_url
FROM fact_event f
JOIN      dim_date     dd ON f.date_key     = dd.date_key
LEFT JOIN dim_site     ds ON f.site_key     = ds.site_key
LEFT JOIN dim_location dl ON f.location_key = dl.location_key
LEFT JOIN dim_device   dv ON f.device_key   = dv.device_key
WHERE f.event_type IN (
    'view_home_page', 'view_landing_page', 'view_listing_page',
    'view_static_page', 'view_my_account', 'view_sorting_relevance'
);

-- ═══════════════════════════════════════════════════════════════════════
-- Layer 3 — the six reports. All read product detail views.
-- ═══════════════════════════════════════════════════════════════════════

-- Most-viewed products.
CREATE VIEW v_top_products_today AS
SELECT report_date, product_id, product_name, COUNT(*) AS view_count
FROM v_product_view_enriched
GROUP BY report_date, product_id, product_name
ORDER BY report_date DESC, view_count DESC;

-- Views by site locale.
CREATE VIEW v_top_countries_today AS
SELECT report_date, country_domain, url_country_name AS country_name, COUNT(*) AS view_count
FROM v_product_view_enriched
GROUP BY report_date, country_domain, url_country_name
ORDER BY report_date DESC, view_count DESC;

-- An empty referrer is direct traffic, not an unknown one.
CREATE VIEW v_top_referrers_today AS
SELECT
    report_date,
    COALESCE(NULLIF(referrer_url, ''), '(direct)') AS referrer_url,
    COUNT(*) AS view_count
FROM v_product_view_enriched
GROUP BY report_date, 2
ORDER BY report_date DESC, view_count DESC;

-- Un-limited: the ranking cutoff belongs to whoever is querying.
CREATE VIEW v_stores_by_country AS
SELECT
    report_date, country_domain, url_country_name AS country_name, store_id,
    COUNT(*) AS view_count
FROM v_product_view_enriched
GROUP BY report_date, country_domain, url_country_name, store_id
ORDER BY report_date DESC, country_domain, view_count DESC;

-- When a product gets looked at.
CREATE VIEW v_product_hourly AS
SELECT report_date, product_id, product_name, hour, COUNT(*) AS view_count
FROM v_product_view_enriched
GROUP BY report_date, product_id, product_name, hour
ORDER BY report_date DESC, product_id, hour;

-- Browser and OS mix over the day.
CREATE VIEW v_device_hourly AS
SELECT report_date, browser, os, device_category, hour, COUNT(*) AS view_count
FROM v_product_view_enriched
GROUP BY report_date, browser, os, device_category, hour
ORDER BY report_date DESC, hour, view_count DESC;
