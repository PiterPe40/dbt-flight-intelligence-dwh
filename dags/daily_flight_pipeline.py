"""
Airflow DAG: Daily Flight Intelligence Pipeline.

Schedule: Daily at 06:00 UTC
Flow:
    1. Fetch flights from OpenSky API (previous day)
    2. Load raw data into BigQuery/DuckDB
    3. Run dbt staging models
    4. Run dbt intermediate models
    5a. Run dbt SQL marts (parallel)
    5b. Run dbt Python models (parallel)
    6. Run dbt tests
    7. Check for military activity anomalies → alert if CRITICAL
"""

import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

# ── Default arguments ────────────────────────
default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 1, 1),
}

# ── Path to dbt project inside container ─────
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"

# Build dbt command prefix with project/profile paths
DBT_CMD = f"cd {DBT_PROJECT_DIR} && dbt"


# ── Task functions ───────────────────────────

def fetch_opensky_flights(**context):
    """Task 1: Fetch flight data from OpenSky for the previous day.

    Uses the logical execution date from Airflow context to determine
    which day to fetch (always the previous day relative to DAG run).
    """
    from helpers.opensky_client import OpenSkyClient

    # Get the execution date (Airflow passes this automatically)
    execution_date = context["logical_date"]
    target_date = execution_date - timedelta(days=1)

    client = OpenSkyClient(
        username=os.getenv("OPENSKY_USERNAME"),
        password=os.getenv("OPENSKY_PASSWORD"),
    )

    flights = client.fetch_daily_flights(target_date)

    # Store flight data for the next task via XCom
    context["ti"].xcom_push(key="flights", value=flights)
    context["ti"].xcom_push(key="source_date", value=target_date.strftime("%Y-%m-%d"))
    context["ti"].xcom_push(key="flight_count", value=len(flights))

    logger.info(f"Fetched {len(flights)} flights for {target_date.date()}")
    return len(flights)


def load_to_raw(**context):
    """Task 2: Load raw flight data into BigQuery/DuckDB.

    Pulls flight data from XCom (Task 1) and loads it into the raw layer.
    """
    from helpers.data_loader import DataLoader

    ti = context["ti"]
    flights = ti.xcom_pull(task_ids="fetch_opensky_flights", key="flights")
    source_date = ti.xcom_pull(task_ids="fetch_opensky_flights", key="source_date")

    if not flights:
        logger.warning("No flights to load — skipping")
        return 0

    loader = DataLoader()
    rows_loaded = loader.load_flights(flights, source_date)
    loader.close()

    logger.info(f"Loaded {rows_loaded} rows to raw layer")
    return rows_loaded


def check_anomalies_and_alert(**context):
    """Task 7: Check for CRITICAL military activity anomalies.

    Queries the mart_military_anomaly_scores table and logs alerts
    for any CRITICAL or HIGH anomaly levels detected.
    """
    use_local = os.getenv("USE_LOCAL_DB", "true").lower() == "true"

    if use_local:
        import duckdb

        db_path = os.getenv("LOCAL_DB_PATH", "./data/flight_intelligence.duckdb")
        conn = duckdb.connect(db_path)

        try:
            anomalies = conn.execute("""
                SELECT date_id, country_code, military_flight_count,
                       z_score, anomaly_level
                FROM marts.mart_military_anomaly_scores
                WHERE anomaly_level IN ('CRITICAL', 'HIGH')
                ORDER BY z_score DESC
                LIMIT 20
            """).fetchdf()
        except Exception as e:
            logger.warning(f"Could not query anomalies: {e}")
            return

        conn.close()
    else:
        from google.cloud import bigquery

        client = bigquery.Client()
        project = os.getenv("GCP_PROJECT_ID")
        dataset = os.getenv("BQ_DATASET", "flight_intelligence")

        query = f"""
            SELECT date_id, country_code, military_flight_count,
                   z_score, anomaly_level
            FROM `{project}.{dataset}_marts.mart_military_anomaly_scores`
            WHERE anomaly_level IN ('CRITICAL', 'HIGH')
            ORDER BY z_score DESC
            LIMIT 20
        """
        anomalies = client.query(query).to_dataframe()

    if anomalies.empty:
        logger.info("No military activity anomalies detected")
        return

    # Log alerts (in production, send to Slack/email)
    critical_count = len(anomalies[anomalies["anomaly_level"] == "CRITICAL"])
    high_count = len(anomalies[anomalies["anomaly_level"] == "HIGH"])

    alert_msg = (
        f"MILITARY ACTIVITY ALERT:\n"
        f"  CRITICAL anomalies: {critical_count}\n"
        f"  HIGH anomalies: {high_count}\n"
        f"  Details:\n"
    )

    for _, row in anomalies.iterrows():
        alert_msg += (
            f"    [{row['anomaly_level']}] {row['country_code']} on {row['date_id']}: "
            f"{row['military_flight_count']} flights (z={row['z_score']:.2f})\n"
        )

    logger.warning(alert_msg)

    # Push alert summary to XCom for downstream monitoring
    context["ti"].xcom_push(key="alert_summary", value=alert_msg)


# ── DAG Definition ───────────────────────────
with DAG(
    dag_id="daily_flight_pipeline",
    default_args=default_args,
    description="Daily ETL pipeline for flight data with military anomaly detection",
    schedule_interval="0 6 * * *",  # Every day at 06:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["flight-intelligence", "dbt", "opensky", "military"],
) as dag:

    # ── Task 1: Fetch from OpenSky API ───────
    fetch_flights = PythonOperator(
        task_id="fetch_opensky_flights",
        python_callable=fetch_opensky_flights,
    )

    # ── Task 2: Load to raw layer ────────────
    load_raw = PythonOperator(
        task_id="load_to_raw",
        python_callable=load_to_raw,
    )

    # ── Task 3: dbt run — staging ────────────
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_CMD} run --select staging --profiles-dir {DBT_PROFILES_DIR}",
    )

    # ── Task 4: dbt run — intermediate ───────
    dbt_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=f"{DBT_CMD} run --select intermediate --profiles-dir {DBT_PROFILES_DIR}",
    )

    # ── Task 5a: dbt run — SQL marts ─────────
    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT_CMD} run --select marts --exclude tag:python --profiles-dir {DBT_PROFILES_DIR}",
    )

    # ── Task 5b: dbt run — Python models ─────
    dbt_python = BashOperator(
        task_id="dbt_run_python_models",
        bash_command=f"{DBT_CMD} run --select tag:python --profiles-dir {DBT_PROFILES_DIR}",
    )

    # ── Task 6: dbt test ─────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_CMD} test --profiles-dir {DBT_PROFILES_DIR}",
    )

    # ── Task 7: Check anomalies & alert ──────
    check_alerts = PythonOperator(
        task_id="check_anomalies_and_alert",
        python_callable=check_anomalies_and_alert,
    )

    # ── Task Dependencies ────────────────────
    # Linear flow: fetch → load → staging → intermediate
    fetch_flights >> load_raw >> dbt_staging >> dbt_intermediate

    # Parallel: SQL marts and Python models run simultaneously
    dbt_intermediate >> [dbt_marts, dbt_python]

    # Both must complete before testing
    [dbt_marts, dbt_python] >> dbt_test >> check_alerts
