-- Analytics warehouse: fact_event (append-only) + 5 dimensions + the ip_locations lookup.
-- fact_event is a plain table for now — no partitioning. Views live in create_views.sql.

-- ═══════════════════════════════════════════════════════════════════════
-- 1. dim_date — calendar spine (2024-01-01 .. 2030-12-31, static SCD1)
-- ═══════════════════════════════════════════════════════════════════════
CREATE TABLE dim_date (
    date_key       INTEGER      PRIMARY KEY,       -- YYYYMMDD
    full_date      DATE         NOT NULL UNIQUE,
    year           SMALLINT     NOT NULL,
    quarter        SMALLINT     NOT NULL,
    month          SMALLINT     NOT NULL,
    month_name     VARCHAR(20)  NOT NULL,
    week_of_year   SMALLINT     NOT NULL,
    day_of_week    SMALLINT     NOT NULL,           -- 1=Monday ... 7=Sunday (ISO)
    day_name       VARCHAR(20)  NOT NULL,
    is_weekend     BOOLEAN      NOT NULL
);
CREATE INDEX ON dim_date (full_date);

INSERT INTO dim_date (date_key, full_date, year, quarter, month, month_name,
                       week_of_year, day_of_week, day_name, is_weekend)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d,
    EXTRACT(YEAR FROM d)::SMALLINT,
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(MONTH FROM d)::SMALLINT,
    TRIM(TO_CHAR(d, 'Month')),
    EXTRACT(WEEK FROM d)::SMALLINT,
    EXTRACT(ISODOW FROM d)::SMALLINT,
    TRIM(TO_CHAR(d, 'Day')),
    EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series('2024-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) AS d;

-- ═══════════════════════════════════════════════════════════════════════
-- 2. dim_site — the URL locale the user is viewing (from the site domain),
--    not real geography.
-- ═══════════════════════════════════════════════════════════════════════
CREATE TABLE dim_site (
    site_key       SERIAL       PRIMARY KEY,
    country_domain VARCHAR(20)  NOT NULL,          -- "cl", "de", "br", "com" (unknown)
    country_iso    VARCHAR(3),
    country_name   VARCHAR(100),
    continent      VARCHAR(50),
    region         VARCHAR(50),
    timezone       VARCHAR(50),
    is_unknown     BOOLEAN      NOT NULL DEFAULT FALSE,
    UNIQUE (country_domain)
);

-- country_name is spelled the way IP2Location spells it ("Viet Nam", "Korea, Republic of") —
-- the views compare this column against dim_location.country_name directly, so a prettier
-- spelling silently turns every visit from that country into a false cross-border hit.
-- 'com'/'local'/'unknown' are not countries: NULL name, so the comparison yields NULL.
INSERT INTO dim_site (country_domain, country_iso, country_name, continent, is_unknown) VALUES
    ('unknown', NULL, NULL,                              NULL,            TRUE),
    ('com',     NULL, NULL,                              NULL,            TRUE),
    ('local',   NULL, NULL,                              NULL,            TRUE),
    ('ae', 'AE', 'United Arab Emirates',                 'Asia',          FALSE),
    ('al', 'AL', 'Albania',                              'Europe',        FALSE),
    ('ar', 'AR', 'Argentina',                            'South America', FALSE),
    ('at', 'AT', 'Austria',                              'Europe',        FALSE),
    ('au', 'AU', 'Australia',                            'Oceania',       FALSE),
    ('az', 'AZ', 'Azerbaijan',                           'Asia',          FALSE),
    ('be', 'BE', 'Belgium',                              'Europe',        FALSE),
    ('bg', 'BG', 'Bulgaria',                             'Europe',        FALSE),
    ('bo', 'BO', 'Bolivia, Plurinational State of',      'South America', FALSE),
    ('br', 'BR', 'Brazil',                               'South America', FALSE),
    ('ca', 'CA', 'Canada',                               'North America', FALSE),
    ('ch', 'CH', 'Switzerland',                          'Europe',        FALSE),
    ('cl', 'CL', 'Chile',                                'South America', FALSE),
    ('cn', 'CN', 'China',                                'Asia',          FALSE),
    ('co', 'CO', 'Colombia',                             'South America', FALSE),
    ('cr', 'CR', 'Costa Rica',                           'North America', FALSE),
    ('cz', 'CZ', 'Czech Republic',                       'Europe',        FALSE),
    ('de', 'DE', 'Germany',                              'Europe',        FALSE),
    ('dk', 'DK', 'Denmark',                              'Europe',        FALSE),
    ('do', 'DO', 'Dominican Republic',                   'North America', FALSE),
    ('ec', 'EC', 'Ecuador',                              'South America', FALSE),
    ('ee', 'EE', 'Estonia',                              'Europe',        FALSE),
    ('es', 'ES', 'Spain',                                'Europe',        FALSE),
    ('fi', 'FI', 'Finland',                              'Europe',        FALSE),
    ('fr', 'FR', 'France',                               'Europe',        FALSE),
    ('gt', 'GT', 'Guatemala',                            'North America', FALSE),
    ('hk', 'HK', 'Hong Kong',                            'Asia',          FALSE),
    ('hn', 'HN', 'Honduras',                             'North America', FALSE),
    ('hr', 'HR', 'Croatia',                              'Europe',        FALSE),
    ('hu', 'HU', 'Hungary',                              'Europe',        FALSE),
    ('ie', 'IE', 'Ireland',                              'Europe',        FALSE),
    ('in', 'IN', 'India',                                'Asia',          FALSE),
    ('is', 'IS', 'Iceland',                              'Europe',        FALSE),
    ('it', 'IT', 'Italy',                                'Europe',        FALSE),
    ('jp', 'JP', 'Japan',                                'Asia',          FALSE),
    ('kr', 'KR', 'Korea, Republic of',                   'Asia',          FALSE),
    ('lt', 'LT', 'Lithuania',                            'Europe',        FALSE),
    ('lv', 'LV', 'Latvia',                               'Europe',        FALSE),
    ('md', 'MD', 'Moldova, Republic of',                 'Europe',        FALSE),
    ('mt', 'MT', 'Malta',                                'Europe',        FALSE),
    ('mx', 'MX', 'Mexico',                               'North America', FALSE),
    ('nl', 'NL', 'Netherlands',                          'Europe',        FALSE),
    ('no', 'NO', 'Norway',                               'Europe',        FALSE),
    ('nz', 'NZ', 'New Zealand',                          'Oceania',       FALSE),
    ('pe', 'PE', 'Peru',                                 'South America', FALSE),
    ('ph', 'PH', 'Philippines',                          'Asia',          FALSE),
    ('pl', 'PL', 'Poland',                               'Europe',        FALSE),
    ('pr', 'PR', 'Puerto Rico',                          'North America', FALSE),
    ('pt', 'PT', 'Portugal',                             'Europe',        FALSE),
    ('ro', 'RO', 'Romania',                              'Europe',        FALSE),
    ('rs', 'RS', 'Serbia',                               'Europe',        FALSE),
    ('se', 'SE', 'Sweden',                               'Europe',        FALSE),
    ('sg', 'SG', 'Singapore',                            'Asia',          FALSE),
    ('si', 'SI', 'Slovenia',                             'Europe',        FALSE),
    ('sk', 'SK', 'Slovakia',                             'Europe',        FALSE),
    ('sv', 'SV', 'El Salvador',                          'North America', FALSE),
    ('th', 'TH', 'Thailand',                             'Asia',          FALSE),
    ('tr', 'TR', 'Turkey',                               'Asia',          FALSE),
    ('ua', 'UA', 'Ukraine',                              'Europe',        FALSE),
    ('uk', 'GB', 'United Kingdom',                       'Europe',        FALSE),
    ('uy', 'UY', 'Uruguay',                              'South America', FALSE),
    ('vn', 'VN', 'Viet Nam',                             'Asia',          FALSE),
    ('za', 'ZA', 'South Africa',                         'Africa',        FALSE)
ON CONFLICT (country_domain) DO UPDATE SET
    country_iso  = EXCLUDED.country_iso,
    country_name = EXCLUDED.country_name,
    continent    = EXCLUDED.continent,
    is_unknown   = EXCLUDED.is_unknown;

-- ═══════════════════════════════════════════════════════════════════════
-- 3. dim_device — junk dimension (browser × os × device_category)
-- ═══════════════════════════════════════════════════════════════════════
CREATE TABLE dim_device (
    device_key       SERIAL       PRIMARY KEY,
    browser          VARCHAR(50)  NOT NULL,
    os               VARCHAR(50)  NOT NULL,
    device_category  VARCHAR(20)  NOT NULL,
    is_mobile        BOOLEAN      NOT NULL,
    UNIQUE (browser, os, device_category)
);

-- Required: user_agent null → UA parser returns ('unknown','unknown','unknown').
INSERT INTO dim_device (browser, os, device_category, is_mobile)
VALUES ('unknown', 'unknown', 'unknown', FALSE);

-- ═══════════════════════════════════════════════════════════════════════
-- 4. dim_product — upsert-on-first-seen; the only dimension populated
--    dynamically from the stream (the other three are seeded once above).
-- ═══════════════════════════════════════════════════════════════════════
CREATE TABLE dim_product (
    product_key      SERIAL        PRIMARY KEY,
    product_id       VARCHAR(50)   NOT NULL UNIQUE,
    -- NULL means "known from the catalog, never actually seen in an event" —
    -- these two columns only get a value from a real stream event (see upsert.py).
    first_seen_at    TIMESTAMPTZ,
    last_seen_at     TIMESTAMPTZ,
    product_name     VARCHAR(500),
    category_id      VARCHAR(50),
    category_name    VARCHAR(200),
    -- Catalog enrichment (backfilled from a scraped export, not from the event stream)
    url              VARCHAR(500),
    sku              VARCHAR(50),
    product_type     VARCHAR(20),
    collection_name  VARCHAR(50),
    gender           VARCHAR(20),
    currency_code    VARCHAR(10),
    price            NUMERIC(15, 2),
    min_price        NUMERIC(15, 2),
    max_price        NUMERIC(15, 2),
    gold_weight      NUMERIC(10, 4)
);
CREATE INDEX ON dim_product (product_id);

-- ═══════════════════════════════════════════════════════════════════════
-- 5. ip_locations — resolution lookup, NOT a dimension: the stream joins through it
--    to reach dim_location. Populated offline by scripts/load_ip_locations.py.
-- ═══════════════════════════════════════════════════════════════════════
CREATE TABLE ip_locations (
    ip       VARCHAR(45)  PRIMARY KEY,
    country  VARCHAR(100),
    region   VARCHAR(100),
    city     VARCHAR(100)
);

-- ═══════════════════════════════════════════════════════════════════════
-- 6. dim_location — real geography from the visitor's IP, unlike dim_site's locale
--    from the URL. Grain is the deduped (country, region, city) combination.
-- ═══════════════════════════════════════════════════════════════════════
CREATE TABLE dim_location (
    location_key           SERIAL       PRIMARY KEY,
    country_name           VARCHAR(100),
    region_name            VARCHAR(100),
    city_name              VARCHAR(100),
    geo_completeness_level SMALLINT,                -- 1=country, 2=+region, 3=full
    has_geo_data           BOOLEAN      NOT NULL,   -- FALSE for the all-NULL combination
    -- NULLS NOT DISTINCT is required: partial geo like (Chile, NULL, NULL) would
    -- otherwise never conflict and the loader would re-insert it every run.
    UNIQUE NULLS NOT DISTINCT (country_name, region_name, city_name)
);
CREATE INDEX ON dim_location (country_name, city_name);

-- ═══════════════════════════════════════════════════════════════════════
-- 7. fact_event — plain table for now (no partitioning). PK is just event_key;
--    idempotency relies on UNIQUE(event_id). Partitioning by report_date later
--    would need report_date added to the PK/unique constraint (Postgres requires
--    the partition key in both).
-- ═══════════════════════════════════════════════════════════════════════
CREATE TABLE fact_event (
    event_key            BIGSERIAL     PRIMARY KEY,
    event_id             VARCHAR(50)   NOT NULL UNIQUE,

    -- Inline, no FK — validated via a static allow-list in the Spark job.
    event_type           VARCHAR(50)   NOT NULL,

    -- FK dims, no REFERENCES declared — avoids FK-check overhead on high-throughput appends.
    date_key             INTEGER       NOT NULL,
    site_key             INTEGER,
    location_key         INTEGER,      -- NULL when the IP resolved to nothing at all
    product_key          INTEGER,
    device_key           INTEGER,

    report_date          DATE          NOT NULL,
    hour                 SMALLINT      NOT NULL CHECK (hour BETWEEN 0 AND 23),
    event_timestamp      TIMESTAMPTZ   NOT NULL,
    event_timestamp_unix BIGINT,

    store_id             VARCHAR(50),
    session_id           VARCHAR(50),
    device_id            VARCHAR(100),
    user_id_db           VARCHAR(100),
    email_hash           VARCHAR(64),
    ip                   VARCHAR(45),

    current_url          TEXT,
    referrer_url         TEXT,
    user_agent           TEXT,
    resolution           VARCHAR(20),

    utm_source           VARCHAR(100),
    utm_medium           VARCHAR(100),

    -- Event-specific fields not typed above. Written with ignoreNullFields=true,
    -- so only keys with a value are included.
    payload              JSONB,

    -- Generated columns for JSONB hot paths — defensive CASE/jsonb_typeof so they
    -- don't crash when payload is missing a field or contains an unexpected null scalar.
    cart_item_count      INTEGER       GENERATED ALWAYS AS (
                                          CASE WHEN jsonb_typeof(payload->'cart_products') = 'array'
                                               THEN jsonb_array_length(payload->'cart_products')
                                          END
                                        ) STORED,
    key_search           TEXT          GENERATED ALWAYS AS
                                        (payload->>'key_search') STORED,
    order_id             VARCHAR(100)  GENERATED ALWAYS AS
                                        (payload->>'order_id') STORED,
    recommendation_position INTEGER    GENERATED ALWAYS AS (
                                          CASE WHEN (payload->>'recommendation_clicked_position') ~ '^-?\d+$'
                                               THEN (payload->>'recommendation_clicked_position')::INTEGER
                                          END
                                        ) STORED,

    event_count          SMALLINT      NOT NULL DEFAULT 1,

    kafka_partition      SMALLINT      NOT NULL,
    kafka_offset         BIGINT        NOT NULL,
    ingested_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX ON fact_event (event_type, report_date);
CREATE INDEX ON fact_event (product_key, report_date) WHERE product_key IS NOT NULL;
CREATE INDEX ON fact_event (site_key, report_date);
CREATE INDEX ON fact_event (location_key, report_date) WHERE location_key IS NOT NULL;
CREATE INDEX ON fact_event (report_date, hour);
CREATE INDEX ON fact_event (device_id, report_date);
CREATE INDEX ON fact_event (order_id) WHERE order_id IS NOT NULL;

-- No GIN index on the full payload column — it would hurt insert throughput,
-- and the generated columns above already cover the hot-path queries.
