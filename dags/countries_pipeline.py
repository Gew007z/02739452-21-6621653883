"""
DAG: countries_pipeline
รวมข้อมูลประชากร พื้นที่ และ GDP ต่อหัวของประเทศทั่วโลกจาก 3 แหล่ง -> PostgreSQL (countrydb)

create_tables
  -> extract_sources (TaskGroup, รันขนาน)
        scrapethissite : scrape HTML (requests + BeautifulSoup)
        restcountries  : REST Countries API
        worldbank      : fetch_indicator (Dynamic Task Mapping, 1 task/ตัวชี้วัด) -> combine
  -> merge_sources        (trigger_rule=all_done : แหล่งไหนล่มก็รวมต่อได้)
  -> transform_dedupe     (จับคู่ชื่อประเทศ -> ISO3 -> ตัดซ้ำข้ามแหล่ง + DQ rules)
  -> quality_gate         (@task.branch)
        ├─ load.upsert_listings_and_countries   (ผ่าน)
        └─ quality_failed                       (ไม่ผ่าน -> เก็บ rejects, ไม่โหลด)
  -> record_run           (บันทึกสถานะลง pipeline_runs ให้หน้าเว็บ /status)
"""
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.utils.state import TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule

import country_etl as etl

SOURCE_TASKS = {
    "scrapethissite": "extract_sources.scrapethissite",
    "restcountries": "extract_sources.restcountries",
    "worldbank": "extract_sources.worldbank.combine",
}
LOAD_TASK = "load.upsert_listings_and_countries"


@dag(
    dag_id="countries_pipeline",
    description="World countries: population, area, GDP per capita from 3 sources",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=15)},
    params={
        "force_dq_fail": Param(False, type="boolean",
                               description="บังคับให้ quality gate ไม่ผ่าน (ทดสอบ branch)"),
                "simulate_source_down": Param(None, type=["null", "string"],
                                      description="ชื่อแหล่งที่จะจำลองว่าล่ม: scrapethissite / restcountries / worldbank"),
    },
    tags=["02739452", "final", "countries"],
)
def countries_pipeline():

    @task
    def create_tables():
        etl.create_tables()

    # ------------------------------------------------------------ extract
    @task_group(group_id="extract_sources")
    def extract_sources():

        @task(task_id="scrapethissite")
        def scrapethissite(params=None):
            if params.get("simulate_source_down") == "scrapethissite":
                raise AirflowFailException("simulated outage: scrapethissite")
            return etl.scrape_countries()

        @task(task_id="restcountries")
        def restcountries(params=None):
            if params.get("simulate_source_down") == "restcountries":
                raise AirflowFailException("simulated outage: restcountries")
            return etl.fetch_restcountries()

        @task_group(group_id="worldbank")
        def worldbank():
            @task
            def list_indicators(params=None):
                if params.get("simulate_source_down") == "worldbank":
                    raise AirflowFailException("simulated outage: worldbank")
                return list(etl.WB_INDICATORS)

            @task
            def fetch_indicator(indicator: str):
                return etl.fetch_wb_indicator(indicator)

            @task
            def combine(results):
                return etl.combine_worldbank(list(results))

            combine(fetch_indicator.expand(indicator=list_indicators()))

        scrapethissite()
        restcountries()
        worldbank()

    # ------------------------------------------------------------ merge
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def merge_sources(ti=None, dag_run=None):
        rows, counts, failed = [], {}, []
        for name, task_id in SOURCE_TASKS.items():
            state = dag_run.get_task_instance(task_id).state
            data = ti.xcom_pull(task_ids=task_id) if state == TaskInstanceState.SUCCESS else None
            if data:
                rows.extend(data)
                counts[name] = len(data)
            else:
                counts[name] = 0
                failed.append(name)
        print(f"rows per source: {counts}  failed: {failed}")
        if not rows:
            raise AirflowFailException("ทุกแหล่งล้มเหลว ไม่มีข้อมูลให้รวม")
        return {"rows": rows, "source_counts": counts, "failed_sources": failed}

    # ------------------------------------------------------------ transform
    @task
    def transform_dedupe(merged: dict):
        result = etl.transform_and_dedupe(merged["rows"])
        valid, rejected = etl.validate(result["listings"])
        print(f"listings={len(result['listings'])} unique_countries={result['unique_countries']} "
              f"cross_source_duplicates={result['cross_source_duplicates']} "
              f"in_all_sources={result['in_all_sources']} valid={len(valid)} rejected={len(rejected)}")
        print("ชื่อจากเว็บที่ scrape แล้วจับคู่ ISO3 ไม่ได้:", result["unmatched_scrape_names"])
        return {
            "valid": valid,
            "rejected": rejected,
            "unique_countries": result["unique_countries"],
            "duplicates": result["cross_source_duplicates"],
            "source_counts": merged["source_counts"],
            "failed_sources": merged["failed_sources"],
            "rows_extracted": len(merged["rows"]),
        }

    # ------------------------------------------------------------ quality gate
    @task.branch
    def quality_gate(data: dict, params=None):
        n_valid, n_bad = len(data["valid"]), len(data["rejected"])
        ratio = n_bad / max(n_valid + n_bad, 1)
        ok = n_valid >= etl.MIN_VALID_ROWS and ratio <= etl.MAX_REJECT_RATIO
        if params.get("force_dq_fail"):
            ok = False
        print(f"DQ: valid={n_valid} (min {etl.MIN_VALID_ROWS}) reject_ratio={ratio:.2%} "
              f"(max {etl.MAX_REJECT_RATIO:.0%}) -> {'PASS' if ok else 'FAIL'}")
        return LOAD_TASK if ok else "quality_failed"

    @task_group(group_id="load")
    def load(data):
        @task
        def upsert_listings_and_countries(data: dict, run_id=None):
            etl.save_rejects(run_id, data["rejected"])
            stats = etl.upsert(data["valid"])
            print(f"UPSERT done: {stats}")
            return stats

        return upsert_listings_and_countries(data)

    @task
    def quality_failed(data: dict, run_id=None):
        n = etl.save_rejects(run_id, data["rejected"])
        print(f"Quality gate FAILED -> ไม่โหลดเข้าตาราง countries, บันทึก rejects {n} แถว")

    # ------------------------------------------------------------ status
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def record_run(data: dict, ti=None, dag_run=None, run_id=None):
        passed = ti.xcom_pull(task_ids="quality_gate") == LOAD_TASK
        failed = data["failed_sources"]
        status = "success" if passed and not failed else ("partial" if passed else "dq_failed")
        etl.record_run({
            "run_id": run_id,
            "logical_date": dag_run.logical_date,
            "started_at": dag_run.start_date,
            "status": status,
            "dq_passed": passed,
            "source_counts": data["source_counts"],
            "failed_sources": failed,
            "rows_extracted": data["rows_extracted"],
            "unique_countries": data["unique_countries"],
            "duplicates": data["duplicates"],
            "rejected": len(data["rejected"]),
            "message": ("failed sources: " + ", ".join(failed)) if failed else "all sources OK",
        })

    # ------------------------------------------------------------ wiring
    merged = merge_sources()
    create_tables() >> extract_sources() >> merged
    data = transform_dedupe(merged)
    gate = quality_gate(data)
    loaded = load(data)
    failed = quality_failed(data)
    gate >> [loaded, failed]
    [loaded, failed] >> record_run(data)


countries_pipeline()
