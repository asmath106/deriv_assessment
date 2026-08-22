"""
Landing loader: loads the raw /data files into the `raw` schema of the local
DuckDB warehouse, tagging each table with `_ingested_at` so dbt source
freshness has something to check against (see
code/dbt/models/staging/_sources.yml).

This is the piece assumed-but-missing from the DAG in trading_pipeline_dag.py
-- Task 1 and Task 2 read /data directly via their own scripts, but
dbt_seed/dbt_source_freshness/dbt_run/dbt_test all expect raw.* tables to
already exist in the warehouse. This script is that step (conceptually
"Task 0" ahead of the three tasks the DAG was specified against).

Uses DuckDB's own JSON/CSV readers with union_by_name=true, which is what
makes CLIENT_DEPOSIT.JSON's malformed DEP012 row (a `credit_card` key
instead of `payment_method`) load cleanly as its own column rather than
crashing the load -- stg_clients_deposit.sql then recovers it via
coalesce(payment_method, credit_card).

Usage:
    python landing_loader.py --data-dir data --db code/dbt/deriv_assessment.duckdb
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

JSON_TABLES = {
    "client_signup": "CLIENT_SIGNUP.JSON",
    "client_profile": "CLIENT_PROFILE.JSON",
    "client_deposit": "CLIENT_DEPOSIT.JSON",
    "client_trades": "CLIENT_TRADES.JSON",
}

CSV_TABLES = {
    "deposits_vendor_20240301": "DEPOSITS_VENDOR_20240301.CSV",
    "deposits_vendor_20240302": "DEPOSITS_VENDOR_20240302.CSV",
    "deposits_vendor_20240303": "DEPOSITS_VENDOR_20240303.CSV",
}


def load(data_dir: Path, db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute("create schema if not exists raw")

    for table, filename in JSON_TABLES.items():
        source_path = (data_dir / filename).resolve()
        con.execute(f"""
            create or replace table raw.{table} as
            select *, current_timestamp as _ingested_at
            from read_json_auto('{source_path.as_posix()}', union_by_name=true)
        """)
        count = con.execute(f"select count(*) from raw.{table}").fetchone()[0]
        print(f"raw.{table}: {count} rows loaded from {filename}")

    for table, filename in CSV_TABLES.items():
        source_path = (data_dir / filename).resolve()
        con.execute(f"""
            create or replace table raw.{table} as
            select *, current_timestamp as _ingested_at
            from read_csv_auto('{source_path.as_posix()}', union_by_name=true)
        """)
        count = con.execute(f"select count(*) from raw.{table}").fetchone()[0]
        print(f"raw.{table}: {count} rows loaded from {filename}")

    con.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--db", type=Path, default=Path("code/dbt/deriv_assessment.duckdb"))
    args = parser.parse_args()
    load(args.data_dir, args.db)


if __name__ == "__main__":
    main()
