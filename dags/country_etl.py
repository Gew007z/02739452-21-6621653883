"""Helper functions for the world-countries pipeline (extract / transform / load).

Sources (public, country-level statistics - no personal data):
  1) scrapethissite.com/pages/simple/  -> web scraping (requests + BeautifulSoup, no Selenium)
  2) REST Countries API v3.1           -> https://restcountries.com
  3) World Bank Indicators API         -> https://api.worldbank.org/v2
"""
import json
import os
import re
import time
import unicodedata
from urllib.robotparser import RobotFileParser

import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup

USER_AGENT = "KU-02739452-final-exam-bot/1.0 (educational project)"
HEADERS = {"User-Agent": USER_AGENT}

SCRAPE_URL = "https://www.scrapethissite.com/pages/simple/"
RESTCOUNTRIES_URL = "https://restcountries.com/v3.1/all"
RESTCOUNTRIES_MIRROR = "https://studies.cs.helsinki.fi/restcountries/api/all"
RESTCOUNTRIES_FIELDS = "name,cca3,capital,region,subregion,population,area,altSpellings"
WB_BASE = "https://api.worldbank.org/v2"
WB_INDICATORS = {
    "SP.POP.TOTL": "population",       # Population, total
    "NY.GDP.PCAP.CD": "gdp_per_capita",  # GDP per capita (current US$)
}

MIN_VALID_ROWS = int(os.getenv("MIN_VALID_ROWS", "100"))
MAX_REJECT_RATIO = float(os.getenv("MAX_REJECT_RATIO", "0.2"))

SOURCES = ["scrapethissite", "restcountries", "worldbank"]

# names used by the scraped site / World Bank that REST Countries spells differently
NAME_ALIASES = {
    "ivory coast": "CIV", "cote d ivoire": "CIV", "czech republic": "CZE", "czechia": "CZE",
    "macedonia": "MKD", "north macedonia": "MKD", "swaziland": "SWZ", "eswatini": "SWZ",
    "east timor": "TLS", "timor leste": "TLS", "cape verde": "CPV", "cabo verde": "CPV",
    "vatican": "VAT", "vatican city": "VAT", "palestinian territory": "PSE", "palestine": "PSE",
    "republic of the congo": "COG", "democratic republic of the congo": "COD",
    "south korea": "KOR", "north korea": "PRK", "russia": "RUS", "laos": "LAO",
    "myanmar": "MMR", "burma": "MMR", "united states": "USA", "united kingdom": "GBR",
    "vietnam": "VNM", "iran": "IRN", "syria": "SYR", "kosovo": "UNK", "turkey": "TUR",
    "u s virgin islands": "VIR", "british virgin islands": "VGB", "micronesia": "FSM",
    "saint martin": "MAF", "sint maarten": "SXM", "curacao": "CUW", "macao": "MAC",
    "hong kong": "HKG", "taiwan": "TWN", "brunei": "BRN", "bolivia": "BOL",
    "venezuela": "VEN", "tanzania": "TZA", "moldova": "MDA", "the bahamas": "BHS",
    "bahamas": "BHS", "gambia": "GMB", "the gambia": "GMB", "reunion": "REU",
    "svalbard and jan mayen": "SJM", "heard island and mcdonald islands": "HMD",
    "saint helena": "SHN", "bonaire saint eustatius and saba": "BES",
}


# --------------------------------------------------------------------------- DB
def get_conn():
    return psycopg2.connect(
        host=os.getenv("COUNTRY_DB_HOST", "postgres"),
        port=int(os.getenv("COUNTRY_DB_PORT", "5432")),
        dbname=os.getenv("COUNTRY_DB_NAME", "countrydb"),
        user=os.getenv("COUNTRY_DB_USER"),
        password=os.getenv("COUNTRY_DB_PASSWORD"),
    )


DDL = """
CREATE TABLE IF NOT EXISTS country_listings (
    source          TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    country_key     TEXT NOT NULL,
    iso3            TEXT,
    name            TEXT NOT NULL,
    capital         TEXT,
    region          TEXT,
    subregion       TEXT,
    population      BIGINT,
    area_km2        NUMERIC(14,2),
    gdp_per_capita  NUMERIC(14,2),
    data_year       INTEGER,
    url             TEXT,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, source_id)
);
CREATE INDEX IF NOT EXISTS idx_listings_country_key ON country_listings(country_key);

CREATE TABLE IF NOT EXISTS countries (
    country_key      TEXT PRIMARY KEY,
    iso3             TEXT,
    name             TEXT NOT NULL,
    capital          TEXT,
    region           TEXT,
    subregion        TEXT,
    population       BIGINT,
    area_km2         NUMERIC(14,2),
    density          NUMERIC(14,2),
    gdp_per_capita   NUMERIC(14,2),
    pop_scrape       BIGINT,
    pop_restcountries BIGINT,
    pop_worldbank    BIGINT,
    wb_year          INTEGER,
    sources          TEXT[] NOT NULL,
    source_count     INTEGER NOT NULL,
    first_seen       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dq_rejects (
    run_id       TEXT NOT NULL,
    source       TEXT NOT NULL,
    source_id    TEXT NOT NULL,
    reason       TEXT NOT NULL,
    payload      JSONB,
    rejected_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, source, source_id)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id           TEXT PRIMARY KEY,
    logical_date     TIMESTAMPTZ,
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    status           TEXT NOT NULL,
    dq_passed        BOOLEAN,
    source_counts    JSONB,
    failed_sources   TEXT[],
    rows_extracted   INTEGER,
    unique_countries INTEGER,
    duplicates       INTEGER,
    rejected         INTEGER,
    message          TEXT
);
"""


def create_tables():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(DDL)


# ------------------------------------------------------------------ helpers
def http_get(url, params=None, timeout=60, tries=3):
    last = None
    for i in range(tries):
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:   # retry with back-off
            last = exc
            time.sleep(2 * (i + 1))
    raise last


def robots_allowed(url):
    """Check robots.txt before scraping (missing robots.txt => allowed)."""
    base = re.match(r"(https?://[^/]+)", url).group(1)
    rp = RobotFileParser()
    try:
        resp = requests.get(base + "/robots.txt", headers=HEADERS, timeout=20)
        if resp.status_code >= 400:
            return True
        rp.parse(resp.text.splitlines())
    except requests.RequestException:
        return True
    return rp.can_fetch(USER_AGENT, url)


def norm_name(name):
    t = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode().lower()
    t = t.replace("&", " and ")
    t = re.sub(r"[^a-z0-9 ]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return re.sub(r"^the ", "", t)


def to_number(text):
    t = (text or "").replace(",", "").strip()
    try:
        return float(t)
    except ValueError:
        return None


# ------------------------------------------------------------------ extract
def parse_scrapethissite(html):
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for div in soup.select("div.country"):
        name = div.select_one("h3.country-name").get_text(" ", strip=True)
        pop = to_number(div.select_one("span.country-population").get_text())
        area = to_number(div.select_one("span.country-area").get_text())
        rows.append({
            "source": "scrapethissite", "source_id": norm_name(name) or name,
            "iso3": None, "name": name,
            "capital": div.select_one("span.country-capital").get_text(strip=True) or None,
            "region": None, "subregion": None,
            "population": int(pop) if pop is not None else None,
            "area_km2": area, "gdp_per_capita": None, "data_year": None,
            "url": SCRAPE_URL,
        })
    return rows


def scrape_countries():
    if not robots_allowed(SCRAPE_URL):
        raise RuntimeError(f"robots.txt disallows {SCRAPE_URL}")
    resp = http_get(SCRAPE_URL, timeout=60)
    resp.encoding = "utf-8"
    return parse_scrapethissite(resp.text)


def fetch_restcountries():
    # API อาจตอบเป็น dict (error/wrapper) แทน list -> ตรวจรูปแบบและลอง endpoint สำรอง
    # restcountries.com v3.1 ถูกยกเลิก (deprecated) และ v5 ต้องใช้ API key
    # จึงใช้ mirror ข้อมูล REST Countries v3.1 ของ University of Helsinki (ไม่ต้องใช้ key) เป็นหลัก
    attempts = [
        (RESTCOUNTRIES_MIRROR, None),
        (RESTCOUNTRIES_URL, {"fields": RESTCOUNTRIES_FIELDS}),
    ]
    data, last_err = [], None
    for url, params in attempts:
        try:
            body = http_get(url, params=params, timeout=60, tries=1).json()
        except Exception as exc:
            last_err = exc
            continue
        if isinstance(body, dict) and isinstance(body.get("data"), list):   # {"data": [...]}
            body = body["data"]
        if isinstance(body, list) and body and isinstance(body[0], dict) and "name" in body[0]:
            data.extend(body)
            break
        else:
            last_err = RuntimeError(f"unexpected response from {url}: {str(body)[:300]}")
            print(last_err)
    if not data:
        raise last_err or RuntimeError("REST Countries returned no data")
    rows = []
    for c in data:
        name = (c.get("name") or {})
        rows.append({
            "source": "restcountries", "source_id": c.get("cca3"),
            "iso3": c.get("cca3"), "name": name.get("common"),
            "capital": (c.get("capital") or [None])[0],
            "region": c.get("region") or None, "subregion": c.get("subregion") or None,
            "population": c.get("population"), "area_km2": c.get("area"),
            "gdp_per_capita": None, "data_year": None,
            "url": f"https://studies.cs.helsinki.fi/restcountries/api/name/{name.get('common')}",
            # used only for name matching in transform (not stored)
            "aliases": [name.get("common"), name.get("official")] + (c.get("altSpellings") or []),
        })
    return rows


def fetch_wb_indicator(indicator):
    """Most recent non-empty value (mrnev=1) of one indicator for every economy."""
    params = {"format": "json", "per_page": 400, "mrnev": 1}
    data = http_get(f"{WB_BASE}/country/all/indicator/{indicator}", params=params, timeout=90).json()
    if not isinstance(data, list) or len(data) < 2 or data[1] is None:
        raise RuntimeError(f"World Bank bad response for {indicator}: {str(data)[:200]}")
    return [{"indicator": indicator, "iso3": d.get("countryiso3code"),
             "name": (d.get("country") or {}).get("value"),
             "value": d.get("value"), "year": d.get("date")}
            for d in data[1] if d.get("countryiso3code")]


def combine_worldbank(indicator_results):
    """Merge indicator rows into one row per country; drop regional aggregates."""
    aggregates = set()
    try:
        meta = http_get(f"{WB_BASE}/country", params={"format": "json", "per_page": 400}).json()
        aggregates = {c["id"] for c in meta[1] if (c.get("region") or {}).get("value") == "Aggregates"}
    except Exception as exc:  # metadata is optional
        print(f"WB metadata unavailable: {exc}")
    by_iso = {}
    for rows in indicator_results:
        for r in rows or []:
            if r["iso3"] in aggregates:
                continue
            c = by_iso.setdefault(r["iso3"], {
                "source": "worldbank", "source_id": r["iso3"], "iso3": r["iso3"],
                "name": r["name"], "capital": None, "region": None, "subregion": None,
                "population": None, "area_km2": None, "gdp_per_capita": None,
                "data_year": None,
                "url": f"https://data.worldbank.org/country/{r['iso3']}",
            })
            field = WB_INDICATORS[r["indicator"]]
            if r["value"] is not None:
                c[field] = int(r["value"]) if field == "population" else round(r["value"], 2)
                if field == "population":
                    c["data_year"] = int(r["year"])
    return list(by_iso.values())


# ---------------------------------------------------------------- transform
def transform_and_dedupe(rows):
    """Match the same country across sources -> one country_key (ISO3 when known)."""
    lookup = dict(NAME_ALIASES)
    for r in rows:                                     # names -> iso3 from APIs
        if r.get("iso3"):
            for alias in (r.get("aliases") or []) + [r.get("name")]:
                if alias and len(alias) > 3:
                    lookup.setdefault(norm_name(alias), r["iso3"])

    listings, seen = [], set()
    for r in rows:
        r = {k: v for k, v in r.items() if k != "aliases"}
        r["name"] = (r.get("name") or "").strip()
        if not r.get("iso3"):
            r["iso3"] = lookup.get(norm_name(r["name"]))
        r["country_key"] = r["iso3"] or ("N:" + norm_name(r["name"]) if r["name"] else "")
        key = (r["source"], r.get("source_id"))
        if key in seen:
            continue
        seen.add(key)
        listings.append(r)

    by_key = {}
    for r in listings:
        if r["country_key"]:
            by_key.setdefault(r["country_key"], set()).add(r["source"])
    unmatched = sorted(r["name"] for r in listings
                       if r["source"] == "scrapethissite" and not r["iso3"])
    return {
        "listings": listings,
        "unique_countries": len(by_key),
        "cross_source_duplicates": sum(1 for r in listings if r["country_key"]) - len(by_key),
        "in_all_sources": sum(1 for v in by_key.values() if len(v) == len(SOURCES)),
        "unmatched_scrape_names": unmatched,
    }


def validate(listings):
    """Data-quality rules. Returns (valid_rows, rejected_rows)."""
    valid, rejected = [], []
    for r in listings:
        reason = None
        pop, area, gdp = r.get("population"), r.get("area_km2"), r.get("gdp_per_capita")
        if r["source"] not in SOURCES:
            reason = "unknown source"
        elif not r.get("source_id"):
            reason = "missing source_id"
        elif not r["name"] or not r["country_key"]:
            reason = "missing country name"
        elif pop is not None and not (0 <= pop < 2_000_000_000):
            reason = "population out of range"
        elif area is not None and not (0 <= area < 20_000_000):
            reason = "area out of range"
        elif gdp is not None and not (0 <= gdp < 1_000_000):
            reason = "gdp per capita out of range"
        elif pop is None and area is None and gdp is None:
            reason = "no numeric data"
        if reason:
            rejected.append({**r, "reason": reason})
        else:
            valid.append(r)
    return valid, rejected


# --------------------------------------------------------------------- load
LISTING_COLS = ["source", "source_id", "country_key", "iso3", "name", "capital", "region",
                "subregion", "population", "area_km2", "gdp_per_capita", "data_year", "url"]

UPSERT_LISTING = f"""
INSERT INTO country_listings ({", ".join(LISTING_COLS)}, fetched_at)
VALUES %s
ON CONFLICT (source, source_id) DO UPDATE SET
    {", ".join(f"{c} = EXCLUDED.{c}" for c in LISTING_COLS[2:])}, fetched_at = now();
"""

# Rebuild the de-duplicated countries table from all listings (one row per country_key)
UPSERT_COUNTRIES = """
WITH agg AS (
  SELECT country_key,
    MAX(iso3) AS iso3,
    (array_agg(name ORDER BY CASE source WHEN 'restcountries' THEN 1
                                         WHEN 'scrapethissite' THEN 2 ELSE 3 END))[1] AS name,
    (array_agg(capital ORDER BY CASE source WHEN 'restcountries' THEN 1 ELSE 2 END)
        FILTER (WHERE capital IS NOT NULL))[1] AS capital,
    MAX(region) AS region, MAX(subregion) AS subregion,
    MAX(population) FILTER (WHERE source = 'scrapethissite') AS pop_scrape,
    MAX(population) FILTER (WHERE source = 'restcountries')  AS pop_rest,
    MAX(population) FILTER (WHERE source = 'worldbank')      AS pop_wb,
    COALESCE(MAX(area_km2) FILTER (WHERE source = 'restcountries'),
             MAX(area_km2) FILTER (WHERE source = 'scrapethissite')) AS area_km2,
    MAX(gdp_per_capita) AS gdp_per_capita,
    MAX(data_year) AS wb_year,
    array_agg(DISTINCT source ORDER BY source) AS sources,
    COUNT(DISTINCT source) AS source_count
  FROM country_listings GROUP BY country_key
)
INSERT INTO countries (country_key, iso3, name, capital, region, subregion, population,
    area_km2, density, gdp_per_capita, pop_scrape, pop_restcountries, pop_worldbank,
    wb_year, sources, source_count, updated_at)
SELECT country_key, iso3, name, capital, region, subregion,
       COALESCE(pop_wb, pop_rest, pop_scrape),
       area_km2,
       CASE WHEN area_km2 > 0 THEN ROUND(COALESCE(pop_wb, pop_rest, pop_scrape) / area_km2, 2) END,
       gdp_per_capita, pop_scrape, pop_rest, pop_wb, wb_year, sources, source_count, now()
FROM agg
ON CONFLICT (country_key) DO UPDATE SET
    iso3 = EXCLUDED.iso3, name = EXCLUDED.name, capital = EXCLUDED.capital,
    region = EXCLUDED.region, subregion = EXCLUDED.subregion,
    population = EXCLUDED.population, area_km2 = EXCLUDED.area_km2,
    density = EXCLUDED.density, gdp_per_capita = EXCLUDED.gdp_per_capita,
    pop_scrape = EXCLUDED.pop_scrape, pop_restcountries = EXCLUDED.pop_restcountries,
    pop_worldbank = EXCLUDED.pop_worldbank, wb_year = EXCLUDED.wb_year,
    sources = EXCLUDED.sources, source_count = EXCLUDED.source_count, updated_at = now();
"""


def upsert(valid_rows):
    values = [tuple(r.get(c) for c in LISTING_COLS) for r in valid_rows]
    template = "(" + ",".join(["%s"] * len(LISTING_COLS)) + ", now())"
    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, UPSERT_LISTING, values, template=template)
        cur.execute(UPSERT_COUNTRIES)
        cur.execute("SELECT COUNT(*) FROM country_listings")
        n_listings = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM countries")
        n_countries = cur.fetchone()[0]
    return {"listings_in_db": n_listings, "countries_in_db": n_countries}


def save_rejects(run_id, rejected):
    if not rejected:
        return 0
    values = [(run_id, r["source"], r.get("source_id") or f"row{i}", r["reason"],
               json.dumps(r, default=str)) for i, r in enumerate(rejected)]
    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO dq_rejects (run_id, source, source_id, reason, payload)
            VALUES %s ON CONFLICT DO NOTHING""", values)
    return len(values)


def record_run(info):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_runs (run_id, logical_date, started_at, finished_at, status,
                dq_passed, source_counts, failed_sources, rows_extracted, unique_countries,
                duplicates, rejected, message)
            VALUES (%(run_id)s, %(logical_date)s, %(started_at)s, now(), %(status)s,
                %(dq_passed)s, %(source_counts)s, %(failed_sources)s, %(rows_extracted)s,
                %(unique_countries)s, %(duplicates)s, %(rejected)s, %(message)s)
            ON CONFLICT (run_id) DO UPDATE SET finished_at = now(), status = EXCLUDED.status,
                dq_passed = EXCLUDED.dq_passed, source_counts = EXCLUDED.source_counts,
                failed_sources = EXCLUDED.failed_sources,
                rows_extracted = EXCLUDED.rows_extracted,
                unique_countries = EXCLUDED.unique_countries, duplicates = EXCLUDED.duplicates,
                rejected = EXCLUDED.rejected, message = EXCLUDED.message
        """, {**info, "source_counts": json.dumps(info.get("source_counts") or {})})
