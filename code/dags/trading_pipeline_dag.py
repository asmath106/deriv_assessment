"""
Airflow DAG for the trading warehouse pipeline.

Task 1 -- validate_cdc_changelog: replay CLIENT_PROFILE_CHANGES.JSONL in
    (client_id, lsn) order via scripts/cdc_processor.py, producing the SCD2
    version history seed consumed by dim_clients. See part1_pipeline.md
    section "Idempotency Strategy" and edge case 7 for why lsn-ordering (not
    arrival order) is required here.

Task 2 -- reconcile_vendor_deposits: ingest DEPOSITS_VENDOR_*.CSV and
    reconcile against the existing warehouse deposit table via
    scripts/deposit_reconciliation.py. Matches on a business key
    (client_id, deposit_date, amount_usd), not deposit_id -- the vendor's
    VDEP* and the warehouse's DEP* are confirmed disjoint ID namespaces.

Task 3 -- dbt_seed / dbt_source_freshness / dbt_run / dbt_test: load the CDC
    seed, confirm the vendor feed landed on schedule (SLA), build staging +
    marts under enforced data contracts, then run data-quality tests.

Task 1 and Task 2 don't share inputs and could run in parallel in
production; they're sequenced here only to mirror the three-task ordering
this DAG was specified against.

Known data-quality findings encoded as dbt tests (see
code/dbt/models/staging/_staging.yml), not just comments:
  - CL099 (vendor deposit VDEP020, 2024-03-03 file) has no client dimension
    row -- relationships test, severity warn. Self-heals via the
    early-arriving-fact placeholder pattern in dim_clients.sql; not a reason
    to fail the build.
  - VDEP001 (2024-03-01 file) has amount_usd = -250.00 -- expression_is_true
    test, default (error) severity. HIGH severity per part1_pipeline.md's
    data quality table ("quarantine + alert") -- this is meant to fail
    dbt_test and should page on-call.
  - CL025's date_of_birth is 1888-12-19 -- accepted_values test on the
    is_dob_suspect flag, severity warn. Flagged for investigation, not
    blocking -- correcting a DOB needs a source-side fix, not a pipeline
    guess.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

REPO_ROOT = "/opt/airflow"  # adjust to the actual mount/deploy path
DBT_PROJECT_DIR = f"{REPO_ROOT}/code/dbt"

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _run_cdc_processor(**context):
    import subprocess
    subprocess.run(
        [
            "python", f"{REPO_ROOT}/code/scripts/cdc_processor.py",
            "--profile", f"{REPO_ROOT}/data/CLIENT_PROFILE.JSON",
            "--changes", f"{REPO_ROOT}/data/CLIENT_PROFILE_CHANGES.JSONL",
            "--out", f"{DBT_PROJECT_DIR}/seeds/client_profile_history.csv",
        ],
        check=True,
    )


def _run_deposit_reconciliation(**context):
    import subprocess
    subprocess.run(
        [
            "python", f"{REPO_ROOT}/code/scripts/deposit_reconciliation.py",
            "--vendor-glob", f"{REPO_ROOT}/data/DEPOSITS_VENDOR_*.CSV",
            "--warehouse", f"{REPO_ROOT}/data/CLIENT_DEPOSIT.JSON",
            "--signup", f"{REPO_ROOT}/data/CLIENT_SIGNUP.JSON",
            "--out", f"{REPO_ROOT}/code/scripts/output/reconciliation_report.csv",
        ],
        check=True,
    )


with DAG(
    dag_id="trading_warehouse_pipeline",
    description="CDC replay + vendor deposit reconciliation + dbt staging/marts build",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 3, 1),
    catchup=False,
    tags=["trading", "warehouse", "cdc", "reconciliation"],
) as dag:

    # Task 1 -- process and validate the CDC changelog with lsn ordering.
    # SLA: this replay shouldn't take long against this data's volume -- an
    # unexpectedly slow run likely means the CDC file grew far beyond the
    # expected daily volume, worth paging on rather than silently waiting.
    validate_cdc_changelog = PythonOperator(
        task_id="validate_cdc_changelog",
        python_callable=_run_cdc_processor,
        sla=timedelta(minutes=10),
    )

    # Task 2 -- ingest and reconcile vendor deposits against the warehouse.
    reconcile_vendor_deposits = PythonOperator(
        task_id="reconcile_vendor_deposits",
        python_callable=_run_deposit_reconciliation,
        sla=timedelta(minutes=10),
    )

    # Task 3a -- load the CDC seed produced by Task 1 into the warehouse.
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt seed --select client_profile_history",
    )

    # Task 3b -- SLA check: did the vendor feed land on schedule? Configured
    # per source in models/staging/_sources.yml (warn_after/error_after).
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt source freshness",
    )

    # Task 3c -- build staging + marts under enforced data contracts. A
    # contract violation (e.g. an unexpected column from schema drift) fails
    # the build here rather than silently flowing downstream.
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select staging marts",
    )

    # Task 3d -- data quality + integrity tests. Error-severity failures
    # (the negative deposit amount) fail this task and should page on-call;
    # warn-severity failures (the orphan client_id, the suspect DOB) surface
    # in the run results without blocking the pipeline.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select staging marts",
    )

    validate_cdc_changelog >> reconcile_vendor_deposits >> dbt_seed
    dbt_seed >> dbt_source_freshness >> dbt_run >> dbt_test
