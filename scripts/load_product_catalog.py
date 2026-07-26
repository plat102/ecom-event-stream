"""One-off script: load the batch project's scraped product catalog export into
stg_product_catalog, then upsert it into dim_product as a base — inserting products
the stream has never seen, and enriching ones it has.
first_seen_at/last_seen_at: stay NULL on a catalog-only row until that happens.

Usage:
    poetry run python scripts/load_product_catalog.py --input data/product_20260329_044128.json
"""
import argparse
import json

from shared.config.settings import settings
from shared.connectors.postgres import PostgresClient

CREATE_STAGING_SQL = """
    CREATE TABLE IF NOT EXISTS stg_product_catalog (
        product_id       VARCHAR(50)     PRIMARY KEY,
        url              VARCHAR(500),
        product_name     VARCHAR(500),
        sku              VARCHAR(50),
        product_type     VARCHAR(20),
        collection_name  VARCHAR(50),
        gender           VARCHAR(20),
        category_id      VARCHAR(50),
        category_name    VARCHAR(200),
        currency_code    VARCHAR(10),
        price            NUMERIC(15, 2),
        min_price        NUMERIC(15, 2),
        max_price        NUMERIC(15, 2),
        gold_weight      NUMERIC(10, 4)
    )
"""

INSERT_STAGING_SQL = """
    INSERT INTO stg_product_catalog (
        product_id, url, product_name, sku, product_type, collection_name, gender,
        category_id, category_name, currency_code, price, min_price, max_price, gold_weight
    )
    VALUES %s
"""

UPSERT_SQL = """
    INSERT INTO dim_product (
        product_id, url, product_name, sku, product_type, collection_name, gender,
        category_id, category_name, currency_code, price, min_price, max_price, gold_weight
    )
    SELECT
        product_id, url, product_name, sku, product_type, collection_name, gender,
        category_id, category_name, currency_code, price, min_price, max_price, gold_weight
    FROM stg_product_catalog
    ON CONFLICT (product_id) DO UPDATE SET
        url = EXCLUDED.url,
        product_name = EXCLUDED.product_name,
        sku = EXCLUDED.sku,
        product_type = EXCLUDED.product_type,
        collection_name = EXCLUDED.collection_name,
        gender = EXCLUDED.gender,
        category_id = EXCLUDED.category_id,
        category_name = EXCLUDED.category_name,
        currency_code = EXCLUDED.currency_code,
        price = EXCLUDED.price,
        min_price = EXCLUDED.min_price,
        max_price = EXCLUDED.max_price,
        gold_weight = EXCLUDED.gold_weight
    -- first_seen_at/last_seen_at deliberately not in this statement at all — a fresh
    -- INSERT leaves them at their column default (NULL); an existing row's values
    -- (real or still NULL) are left exactly as they were.
"""


def _unknown_if_blank(value, invalid=()):
    """Empty/None/known-garbage -> "unknown" — this project's own convention for
    categorical fields with no clean value (see enrich.py's country_domain/browser/os).

    Compares via str() — some rows have `gender` as a JSON boolean `false` rather than
    the string "false"/"False", so a direct `value in invalid` check would miss it.
    """
    if value in (None, ""):
        return "unknown"
    return "unknown" if str(value).lower() in invalid else value


def _to_number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_price(value):
    number = _to_number(value)
    return None if number is None or number <= 0 else number


def clean_row(r):
    # Source field is named "category" but it's the category ID; "0"/0/"" is a
    # "no category" sentinel in this export, not a real category — stays NULL
    # (unlike product_type/gender, there's no meaningful "unknown id" to substitute).
    category_id = r.get("category")
    category_id = None if category_id in (None, "", 0, "0") else str(category_id)
    category_name = "unknown" if category_id is None else _unknown_if_blank(r.get("category_name"))

    return (
        str(r["product_id"]),
        r.get("url") or None,
        r.get("product_name") or None,
        r.get("sku") or None,
        _unknown_if_blank(r.get("product_type"), invalid=("-1", "--_select_--")),
        r.get("collection") or None,  # JSON field is "collection", dim column is collection_name
        _unknown_if_blank(r.get("gender"), invalid=("false",)),
        category_id,
        category_name,
        r.get("currency_code") or None,
        _to_price(r.get("price")),
        _to_price(r.get("min_price")),
        _to_price(r.get("max_price")),
        _to_number(r.get("gold_weight")),
    )


def load_catalog_rows(path):
    with open(path) as f:
        records = json.load(f)

    seen_ids = set()
    rows = []
    for r in records:
        if r.get("status") != "success":
            continue

        product_id = str(r["product_id"])
        if product_id in seen_ids:
            print(f"WARN: duplicate product_id in source file, keeping first: {product_id}")
            continue
        seen_ids.add(product_id)

        rows.append(clean_row(r))

    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    args = parser.parse_args()

    rows = load_catalog_rows(args.input)
    print(f"{len(rows)} usable catalog rows (status=success, deduped)")

    with PostgresClient(settings) as pg:
        pg.execute(CREATE_STAGING_SQL)
        pg.execute("TRUNCATE stg_product_catalog")
        pg.execute_values(INSERT_STAGING_SQL, rows)
        print("loaded into stg_product_catalog")

        product_ids = [r[0] for r in rows]
        existing = pg.fetch_one(
            "SELECT COUNT(*) FROM dim_product WHERE product_id = ANY(%s)", (product_ids,)
        )[0]
        print(f"{existing} already exist in dim_product (will be enriched)")
        print(f"{len(rows) - existing} are new (will be inserted as catalog-only rows)")

        affected = pg.execute(UPSERT_SQL)
        print(f"{affected} rows upserted into dim_product")
        if affected != len(rows):
            print("WARN: upserted count != catalog row count — investigate")


if __name__ == "__main__":
    main()
