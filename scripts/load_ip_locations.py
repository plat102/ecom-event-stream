"""Resolve fact_event's IPs against an IP2Location BIN, fill ip_locations, rebuild
dim_location from the distinct geo combinations, backfill location_key.

Incremental: only IPs absent from ip_locations are resolved, so re-runs are cheap.
Unresolvable IPs get one shared all-NULL row; an IP never looked up stays location_key NULL.

Usage:
    poetry run python scripts/load_ip_locations.py --input data/<ip2location-db>.BIN
"""
import argparse

import IP2Location

from shared.config.settings import settings
from shared.connectors.postgres import PostgresClient

# Only addresses the warehouse has seen — resolving the whole BIN would be millions of dead rows.
SELECT_UNRESOLVED_IPS_SQL = """
    SELECT DISTINCT f.ip
    FROM fact_event f
    LEFT JOIN ip_locations il ON il.ip = f.ip
    WHERE f.ip IS NOT NULL AND il.ip IS NULL
"""

INSERT_IP_LOCATIONS_SQL = """
    INSERT INTO ip_locations (ip, country, region, city)
    VALUES %s
    ON CONFLICT (ip) DO NOTHING
"""

# Needs dim_location's UNIQUE to be NULLS NOT DISTINCT, or partial geo re-inserts every run.
REBUILD_DIM_LOCATION_SQL = """
    INSERT INTO dim_location (
        country_name, region_name, city_name, geo_completeness_level, has_geo_data
    )
    SELECT DISTINCT
        country,
        region,
        city,
        CASE
            WHEN city IS NOT NULL THEN 3
            WHEN region IS NOT NULL THEN 2
            WHEN country IS NOT NULL THEN 1
        END,
        country IS NOT NULL
    FROM ip_locations
    ON CONFLICT (country_name, region_name, city_name) DO NOTHING
"""

# IS NOT DISTINCT FROM, not = : the geo columns are nullable on both sides.
BACKFILL_LOCATION_KEY_SQL = """
    UPDATE fact_event f
    SET location_key = dl.location_key
    FROM ip_locations il
    JOIN dim_location dl
      ON dl.country_name IS NOT DISTINCT FROM il.country
     AND dl.region_name  IS NOT DISTINCT FROM il.region
     AND dl.city_name    IS NOT DISTINCT FROM il.city
    WHERE f.ip = il.ip
      AND f.location_key IS NULL
"""


def _clean(value):
    """IP2Location returns "-" for a missing field and a message string for a bad query."""
    if value in (None, "", "-"):
        return None
    if value.startswith("INVALID") or value.startswith("This method"):
        return None
    return value


def resolve_ips(ips, bin_path):
    database = IP2Location.IP2Location(bin_path)
    rows = []
    unresolved = 0

    for ip in ips:
        try:
            record = database.get_all(ip)
            country = _clean(record.country_long)
            region = _clean(record.region)
            city = _clean(record.city)
        except Exception as exc:  # malformed address, IPv6 outside the DB's range, ...
            print(f"WARN: could not resolve {ip}: {exc}")
            country = region = city = None

        if country is None:
            unresolved += 1
        rows.append((ip, country, region, city))

    return rows, unresolved


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="path to the IP2Location BIN database")
    args = parser.parse_args()

    with PostgresClient(settings) as pg:
        ips = [row[0] for row in pg.fetch_all(SELECT_UNRESOLVED_IPS_SQL)[0]]
        print(f"{len(ips)} distinct IPs in fact_event not yet in ip_locations")

        if ips:
            rows, unresolved = resolve_ips(ips, args.input)
            rate = unresolved / len(rows) * 100
            print(f"resolved {len(rows) - unresolved}/{len(rows)} ({100 - rate:.2f}% with country)")
            pg.execute_values(INSERT_IP_LOCATIONS_SQL, rows)
            print("loaded into ip_locations")

        inserted = pg.execute(REBUILD_DIM_LOCATION_SQL)
        total_geo = pg.fetch_one("SELECT COUNT(*) FROM dim_location")[0]
        print(f"{inserted} new geo combinations, dim_location now {total_geo} rows")

        updated = pg.execute(BACKFILL_LOCATION_KEY_SQL)
        print(f"{updated} fact_event rows backfilled with location_key")

        still_null = pg.fetch_one(
            "SELECT COUNT(*) FROM fact_event WHERE location_key IS NULL"
        )[0]
        if still_null:
            print(f"{still_null} rows still have no location_key (IP absent from ip_locations)")


if __name__ == "__main__":
    main()
