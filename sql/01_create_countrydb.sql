-- รันอัตโนมัติครั้งแรกที่สร้าง container postgres (volume ว่าง)
-- แยกฐานข้อมูลของงาน (countrydb) ออกจากฐานข้อมูล metadata ของ Airflow
CREATE DATABASE countrydb;
