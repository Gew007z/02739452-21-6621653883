# ทดสอบเว็บ + เก็บภาพหน้าจออัตโนมัติ (Playwright)

ต้องเปิดระบบไว้ก่อน (`docker compose up -d`) และ DAG รันสำเร็จแล้วอย่างน้อย 1 รอบ

รันจากโฟลเดอร์ repo (PowerShell):

```powershell
docker run --rm -v "${PWD}:/work" -w /work mcr.microsoft.com/playwright/python:v1.48.0-jammy `
  bash -c "pip install -q playwright==1.48.0 && python tests/test_webapp.py"
```

สิ่งที่ตรวจ: dashboard โหลดได้, KPI ≥ 100 ประเทศ, กราฟ 4 อัน, ตัวกรองภูมิภาค/ค้นหา/ครบ 3 แหล่ง,
หน้า /status, ไม่มี console error, ล็อกอิน Airflow และ scheduler healthy

ภาพที่ได้ใน `tests/shots/`: `01_dashboard`, `02_filter_asia`, `03_status`, `04_airflow_grid`, `05_airflow_graph`

ถ้าตั้งรหัส Airflow ไม่ใช่ค่าใน `.env.example` ให้เพิ่ม `-e AIRFLOW_PASSWORD=<รหัส>` ในคำสั่ง docker run
