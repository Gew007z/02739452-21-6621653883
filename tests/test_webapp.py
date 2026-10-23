"""End-to-end test + screenshots for the countries pipeline (Playwright, Python).

รันด้วย Docker (ไม่ต้องลง Python ในเครื่อง) — ดู tests/README.md
ผลลัพธ์: ตาราง PASS/FAIL ใน terminal และภาพหน้าจอใน tests/shots/
"""
import os
import re
import sys

from playwright.sync_api import sync_playwright

WEB = os.getenv("WEB_URL", "http://host.docker.internal:5000")
AIRFLOW = os.getenv("AIRFLOW_URL", "http://host.docker.internal:8080")
AF_USER = os.getenv("AIRFLOW_USER", "admin")
AF_PASS = os.getenv("AIRFLOW_PASSWORD", "change_me")   # ค่าทดสอบจาก .env.example
DAG = "countries_pipeline"
SHOTS = os.getenv("SHOTS_DIR", "/work/tests/shots")
os.makedirs(SHOTS, exist_ok=True)

results = []


def check(name, cond, note=""):
    results.append((name, bool(cond), note))


def num(text):
    m = re.search(r"[\d,]+", text or "")
    return int(m.group(0).replace(",", "")) if m else -1


def shot(page, name, full=True):
    path = f"{SHOTS}/{name}.png"
    page.screenshot(path=path, full_page=full)
    return path


with sync_playwright() as p:
    browser = p.chromium.launch()
    ctx = browser.new_context(viewport={"width": 1440, "height": 900}, locale="th-TH")
    page = ctx.new_page()
    errors = []
    page.on("console", lambda m: m.type == "error" and errors.append(m.text))
    page.on("response", lambda r: r.status >= 400 and errors.append(f"{r.status} {r.url}"))

    # ---------------------------------------------------------------- dashboard
    r = page.goto(WEB + "/", wait_until="networkidle")
    check("Dashboard โหลดได้ (HTTP 200)", r and r.status == 200, f"status={r.status if r else None}")
    kpis = page.locator(".kpi .v").all_inner_texts()
    total = num(kpis[0]) if kpis else -1
    check("KPI จำนวนประเทศ ≥ 100", total >= 100, f"countries={total}")
    check("มี KPI ครบ 5 ตัว", len(kpis) == 5, f"kpis={kpis}")
    check("มีกราฟ 4 อัน (canvas)", page.locator("canvas").count() == 4)
    rows_all = page.locator("table tbody tr").count()
    check("ตารางมีข้อมูล", rows_all > 0, f"rows={rows_all}")
    page.wait_for_timeout(1500)                       # ให้ Chart.js วาดเสร็จ
    shot(page, "01_dashboard")

    # ---------------------------------------------------------------- filters
    page.goto(WEB + "/?region=Asia", wait_until="networkidle")
    regions = page.locator("table tbody tr td:nth-child(4)").all_inner_texts()
    asia = num(page.locator(".kpi .v").first.inner_text())
    check("ตัวกรองภูมิภาค = Asia", regions and all(x == "Asia" for x in regions),
          f"rows={len(regions)}, KPI={asia}")
    check("KPI เปลี่ยนตามตัวกรอง", 0 < asia < total, f"{asia} < {total}")
    page.wait_for_timeout(1200)
    shot(page, "02_filter_asia", full=False)

    page.goto(WEB + "/?q=Thailand", wait_until="networkidle")
    check("ค้นหา Thailand เจอ", page.get_by_text("Thailand", exact=True).count() > 0)

    page.goto(WEB + "/?multi=1", wait_until="networkidle")
    tag_counts = [row.locator(".tag").count() for row in page.locator("table tbody tr").all()[:50]]
    check("ตัวกรองพบครบ 3 แหล่ง", tag_counts and all(c == 3 for c in tag_counts),
          f"rows checked={len(tag_counts)}")

    # ---------------------------------------------------------------- status page
    r = page.goto(WEB + "/status", wait_until="networkidle")
    check("หน้า /status โหลดได้", r and r.status == 200)
    statuses = set(page.locator("table td.st").all_inner_texts())
    check("มีประวัติการรันใน pipeline_runs", len(statuses) > 0, f"statuses={sorted(statuses)}")
    shot(page, "03_status")

    web_errors = [e for e in errors if WEB.split("//")[1] in e or "Uncaught" in e]
    check("ไม่มี console/HTTP error บนหน้าเว็บ", not web_errors, "; ".join(web_errors[:3]))

    # ---------------------------------------------------------------- airflow
    try:
        page.goto(AIRFLOW + "/login/", wait_until="networkidle")
        page.fill("#username", AF_USER)
        page.fill("#password", AF_PASS)
        page.click("input[type=submit], button[type=submit]")
        page.wait_for_load_state("networkidle")
        r = page.goto(f"{AIRFLOW}/dags/{DAG}/grid", wait_until="networkidle")
        check("ล็อกอิน Airflow + เปิด Grid ได้", r and r.status == 200 and "login" not in page.url)
        page.wait_for_timeout(2500)
        # กาง TaskGroup extract_sources ให้เห็นแต่ละแหล่ง
        try:
            page.get_by_text("extract_sources", exact=True).first.click()
            page.wait_for_timeout(800)
        except Exception:
            pass
        shot(page, "04_airflow_grid", full=False)

        # Graph view: เลือกรอบล่าสุดที่สำเร็จทั้งหมด แล้วเปิดแท็บ Graph
        page.get_by_role("tab", name=re.compile("Graph")).first.click()
        page.wait_for_timeout(2500)
        try:
            page.locator("button[title='fit view'], .react-flow__controls-fitview").first.click()
            page.wait_for_timeout(800)
        except Exception:
            pass
        shot(page, "05_airflow_graph", full=False)

        health = page.request.get(AIRFLOW + "/health").json()
        check("Airflow scheduler healthy", health.get("scheduler", {}).get("status") == "healthy",
              str(health.get("scheduler")))
    except Exception as exc:
        check("Airflow UI", False, f"{type(exc).__name__}: {exc}"[:150])

    browser.close()

# ---------------------------------------------------------------- report
w = max(len(n) for n, _, _ in results) + 2
print("\n" + "test".ljust(w) + "result  note")
print("-" * (w + 40))
for name, ok, note in results:
    print(name.ljust(w) + ("PASS    " if ok else "FAIL    ") + note)
passed = sum(ok for _, ok, _ in results)
print(f"\n{passed}/{len(results)} passed · screenshots -> tests/shots/")
sys.exit(0 if passed == len(results) else 1)
