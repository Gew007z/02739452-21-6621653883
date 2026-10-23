"""World countries dashboard - อ่านข้อมูลจาก PostgreSQL (countrydb) โดยตรง"""
import os

import psycopg2
import psycopg2.extras
from flask import Flask, render_template, request

app = Flask(__name__)

SOURCES = ["scrapethissite", "restcountries", "worldbank"]
SOURCE_LABEL = {"scrapethissite": "scrapethissite.com (scraping)",
                "restcountries": "REST Countries API",
                "worldbank": "World Bank API"}
SORTS = {
    "population": "population DESC NULLS LAST",
    "gdp": "gdp_per_capita DESC NULLS LAST",
    "area": "area_km2 DESC NULLS LAST",
    "density": "density DESC NULLS LAST",
    "name": "name",
    "sources": "source_count DESC, name",
}


def query(sql, params=None):
    conn = psycopg2.connect(
        host=os.getenv("COUNTRY_DB_HOST", "localhost"),
        port=int(os.getenv("COUNTRY_DB_PORT", "5432")),
        dbname=os.getenv("COUNTRY_DB_NAME", "countrydb"),
        user=os.getenv("COUNTRY_DB_USER"),
        password=os.getenv("COUNTRY_DB_PASSWORD"),
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or {})
            return cur.fetchall()
    finally:
        conn.close()


def table_exists(name):
    return query("SELECT to_regclass(%(n)s) IS NOT NULL AS ok", {"n": f"public.{name}"})[0]["ok"]


@app.template_filter("num")
def num(v, digits=0):
    if v is None:
        return "-"
    return f"{float(v):,.{digits}f}"


@app.route("/")
def index():
    f = {
        "q": request.args.get("q", "").strip(),
        "region": request.args.get("region", ""),
        "source": request.args.get("source", ""),
        "min_pop": request.args.get("min_pop", ""),
        "multi": request.args.get("multi", ""),
        "sort": request.args.get("sort", "population"),
    }
    if not table_exists("countries"):
        return render_template("index.html", empty=True, f=f)

    where, params = ["TRUE"], {}
    if f["q"]:
        where.append("(name ILIKE %(q)s OR capital ILIKE %(q)s OR iso3 ILIKE %(q)s)")
        params["q"] = f"%{f['q']}%"
    if f["region"]:
        where.append("region = %(region)s")
        params["region"] = f["region"]
    if f["source"] in SOURCES:
        where.append("%(source)s = ANY(sources)")
        params["source"] = f["source"]
    if f["min_pop"].isdigit():
        where.append("population >= %(min_pop)s")
        params["min_pop"] = int(f["min_pop"]) * 1_000_000
    if f["multi"]:
        where.append("source_count = 3")
    w = " AND ".join(where)
    order = SORTS.get(f["sort"], SORTS["population"])

    kpi = query(f"""
        SELECT COUNT(*) AS countries,
               SUM(population) AS population,
               ROUND(AVG(gdp_per_capita)) AS avg_gdp,
               COUNT(*) FILTER (WHERE source_count = 3) AS all3,
               MAX(wb_year) AS wb_year
        FROM countries WHERE {w}""", params)[0]
    listings = query("SELECT COUNT(*) AS n FROM country_listings")[0]["n"]
    rows = query(f"SELECT * FROM countries WHERE {w} ORDER BY {order} LIMIT 300", params)

    by_region = query(f"""SELECT COALESCE(region,'(ไม่ระบุ)') AS label, SUM(population) AS n
                          FROM countries WHERE {w} GROUP BY 1 ORDER BY 2 DESC NULLS LAST""", params)
    by_source = query(f"""SELECT s AS label, COUNT(*) AS n FROM countries, unnest(sources) s
                          WHERE {w} GROUP BY s ORDER BY s""", params)
    top_gdp = query(f"""SELECT name AS label, gdp_per_capita AS n FROM countries
                        WHERE {w} AND gdp_per_capita IS NOT NULL
                        ORDER BY gdp_per_capita DESC LIMIT 10""", params)
    overlap = query(f"""SELECT source_count AS label, COUNT(*) AS n FROM countries WHERE {w}
                        GROUP BY 1 ORDER BY 1""", params)
    regions = [r["region"] for r in query(
        "SELECT DISTINCT region FROM countries WHERE region IS NOT NULL ORDER BY 1")]
    last_run = query("SELECT * FROM pipeline_runs ORDER BY finished_at DESC LIMIT 1") \
        if table_exists("pipeline_runs") else []

    def pack(rs, fmt=str):
        return {"labels": [fmt(r["label"]) for r in rs], "values": [float(r["n"] or 0) for r in rs]}

    charts = {"region": pack(by_region), "source": pack(by_source), "gdp": pack(top_gdp),
              "overlap": pack(overlap, lambda x: f"{x} แหล่ง")}
    return render_template("index.html", empty=False, f=f, kpi=kpi, listings=listings,
                           rows=rows, charts=charts, sources=SOURCES, labels=SOURCE_LABEL,
                           regions=regions, last_run=last_run[0] if last_run else None)


@app.route("/status")
def status():
    ready = table_exists("pipeline_runs")
    runs, per_source, rejects = [], [], []
    if ready:
        runs = query("SELECT * FROM pipeline_runs ORDER BY finished_at DESC LIMIT 20")
        per_source = query("""SELECT source, COUNT(*) AS n, MAX(fetched_at) AS last_fetch
                              FROM country_listings GROUP BY source ORDER BY source""")
        rejects = query("""SELECT reason, source, COUNT(*) AS n FROM dq_rejects
                           GROUP BY reason, source ORDER BY n DESC LIMIT 20""")
    return render_template("status.html", ready=ready, runs=runs, per_source=per_source,
                           rejects=rejects, sources=SOURCES, labels=SOURCE_LABEL)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
