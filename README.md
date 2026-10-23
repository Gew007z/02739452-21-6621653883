# ระบบรวบรวมข้อมูลประเทศทั่วโลกจากหลายแหล่ง (World Countries Pipeline)

ข้อสอบปลายภาค 02739452 แพลตฟอร์มข้อมูลขนาดใหญ่และการวิเคราะห์ · ภาคต้น 2569

**หัวข้อ:** ข้อมูลประชากร พื้นที่ และเศรษฐกิจ (GDP ต่อหัว) ของประเทศทั่วโลก รวบรวมจาก 3 แหล่ง ด้วย Apache Airflow → PostgreSQL → เว็บ dashboard (Flask)

## แหล่งข้อมูล

| # | แหล่ง | วิธีดึง | ข้อมูลที่ได้ | จำนวนโดยประมาณ |
|---|---|---|---|---|
| 1 | [scrapethissite.com/pages/simple](https://www.scrapethissite.com/pages/simple/) | **Web scraping** (requests + BeautifulSoup, ไม่ใช้ Selenium) ตรวจ robots.txt ก่อนดึง | ชื่อ เมืองหลวง ประชากร พื้นที่ | 250 |
| 2 | [REST Countries](https://studies.cs.helsinki.fi/restcountries/) (mirror ของ University of Helsinki เพราะ restcountries.com v3.1 ถูกยกเลิกและ v5 ต้องใช้ API key) | REST API (JSON) | ชื่อ ISO3 เมืองหลวง ภูมิภาค ประชากร พื้นที่ | 250 |
| 3 | [World Bank Indicators API](https://datahelpdesk.worldbank.org/knowledgebase/articles/889392) | REST API (JSON) ตัวชี้วัด `SP.POP.TOTL`, `NY.GDP.PCAP.CD` (ค่าล่าสุด) | ประชากร GDP ต่อหัว ปีข้อมูล | ~217 (ตัดกลุ่มภูมิภาค/aggregate ออก) |

รวมกว่า 700 รายการดิบ → ประมาณ 250 ประเทศ/ดินแดนหลังตัดซ้ำ · เป็นสถิติระดับประเทศ **ไม่มีข้อมูลส่วนบุคคล** สอดคล้อง PDPA

## สถาปัตยกรรม DAG `countries_pipeline`

```
create_tables
  └─> extract_sources  (TaskGroup — 3 แหล่งรันขนานกัน)
        ├─ scrapethissite                   web scraping
        ├─ restcountries                    REST API
        └─ worldbank: list_indicators ─> fetch_indicator[ ] (Dynamic Task Mapping, 1 task/ตัวชี้วัด) ─> combine
  └─> merge_sources      trigger_rule=all_done → แหล่งไหนล่ม ยังรวมแหล่งที่เหลือต่อได้
  └─> transform_dedupe   จับคู่ชื่อประเทศ → รหัส ISO3 → ตัดรายการซ้ำข้ามแหล่ง + ตรวจกฎ DQ
  └─> quality_gate       @task.branch
        ├─ load.upsert_listings_and_countries   (ผ่าน) UPSERT ON CONFLICT
        └─ quality_failed                       (ไม่ผ่าน) เก็บแถวเสียลง dq_rejects ไม่โหลด
  └─> record_run         บันทึกผลลง pipeline_runs (ใช้ในหน้า /status)
```

**ตารางใน PostgreSQL (`countrydb`)**
- `country_listings` — 1 แถวต่อ 1 รายการจากแต่ละแหล่ง, PK `(source, source_id)` → UPSERT
- `countries` — 1 แถวต่อประเทศหลังตัดซ้ำ (PK `country_key` = ISO3) เก็บประชากรที่แต่ละแหล่งรายงานไว้เทียบกัน และ `sources[]` → UPSERT
- `dq_rejects` — แถวที่ไม่ผ่านการตรวจคุณภาพ พร้อมเหตุผล
- `pipeline_runs` — สถานะการรันแต่ละรอบ

**การตัดซ้ำข้ามแหล่ง:** REST Countries และ World Bank มีรหัส ISO3 อยู่แล้ว ส่วนเว็บที่ scrape มีแค่ชื่อ → normalize ชื่อ (ตัวพิมพ์เล็ก ตัดสำเนียง/เครื่องหมาย) แล้วเทียบกับชื่อทางการและ altSpellings จาก REST Countries + ตาราง alias (เช่น Ivory Coast → CIV) จนได้ ISO3 เดียวกัน

**กฎตรวจคุณภาพ (DQ):** ต้องมีชื่อและ source_id, ประชากร 0–2 พันล้าน, พื้นที่ 0–20 ล้าน km², GDP ต่อหัว 0–1 ล้าน US$, ต้องมีตัวเลขอย่างน้อย 1 ค่า → ผ่านเมื่อแถวถูกต้อง ≥ 100 และแถวเสีย ≤ 20%

## วิธีรัน

ต้องมี Docker Desktop (หรือ Docker Engine + Compose v2)

```bash
git clone <repo-url>
cd <repo>
cp .env.example .env          # แก้รหัสผ่านใน .env ได้ตามต้องการ
docker compose up -d --build
```

รอประมาณ 1–2 นาทีให้ `airflow-init` เสร็จ แล้วเปิด

| บริการ | URL | หมายเหตุ |
|---|---|---|
| Airflow | http://localhost:8080 | user/pass ตาม `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` ใน `.env` |
| Dashboard | http://localhost:5000 | KPI, ตัวกรอง, กราฟ 4 อัน, ตาราง |
| Pipeline status | http://localhost:5000/status | สถานะแต่ละแหล่ง, ประวัติการรัน, เหตุผลที่ไม่ผ่าน DQ |
| PostgreSQL | localhost:5433 | database `countrydb` |

DAG `countries_pipeline` ไม่ถูก pause ตั้งแต่สร้าง จึงรันอัตโนมัติ 1 รอบ หรือกด ▶ Trigger DAG เองก็ได้

### ทดสอบความสามารถแต่ละระดับ

- **UPSERT/รันซ้ำแถวไม่เพิ่ม:** Trigger ซ้ำ แล้วรันคำสั่งด้านล่าง จำนวนแถวเท่าเดิม
- **แหล่งล่มแล้วยังรวมต่อได้:** Trigger DAG w/ config `{"simulate_source_down": "restcountries"}` → task restcountries สีแดง แต่ merge/load ยังทำงาน หน้า /status แสดง `partial`
- **Branch เมื่อ DQ ไม่ผ่าน:** Trigger w/ config `{"force_dq_fail": true}` → ไปทาง `quality_failed`, `load` ถูก skip

```bash
docker compose exec postgres psql -U airflow -d countrydb -c "SELECT COUNT(*) FROM countries; SELECT source, COUNT(*) FROM country_listings GROUP BY 1;"
```

## โครงสร้างไฟล์

```
dags/countries_pipeline.py   DAG
dags/country_etl.py          ฟังก์ชัน extract / transform / DQ / load
web/app.py                   Flask dashboard (อ่าน PostgreSQL โดยตรง)
web/templates/               หน้า dashboard และ status
sql/01_create_countrydb.sql  สร้างฐานข้อมูล countrydb ตอนเริ่ม postgres
docker-compose.yml  requirements.txt  .env.example
```

ไม่มีรหัสผ่านหรือ API key อยู่ใน repo (ใช้ `.env` ซึ่งอยู่ใน `.gitignore`)
