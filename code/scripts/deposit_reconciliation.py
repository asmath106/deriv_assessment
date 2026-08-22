"""
Reconciles the vendor deposit feed (DEPOSITS_VENDOR_*.CSV) against the
existing warehouse deposit table (CLIENT_DEPOSIT.JSON).

The two systems use disjoint deposit_id namespaces -- vendor: VDEP*,
warehouse: DEP* -- confirmed zero ID overlap across the full sample data.
So matching is done on a business key (client_id, deposit_date, amount_usd),
not deposit_id.

Usage:
    python deposit_reconciliation.py \\
        --vendor-glob "data/DEPOSITS_VENDOR_*.CSV" \\
        --warehouse data/CLIENT_DEPOSIT.JSON \\
        --signup data/CLIENT_SIGNUP.JSON \\
        --out code/scripts/output/reconciliation_report.csv
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
from pathlib import Path

BUSINESS_KEY = ("client_id", "deposit_date", "amount_usd")
COMPARE_FIELDS = ("fee_usd", "payment_method", "status", "processing_days")


def load_vendor_deposits(glob_pattern: str) -> list:
    rows = []
    for path in sorted(glob.glob(glob_pattern)):
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # 2024-03-02 delivery renames payment_method -> method
                if "method" in row and "payment_method" not in row:
                    row["payment_method"] = row.pop("method")
                row["amount_usd"] = float(row["amount_usd"])
                row["source_file"] = Path(path).name
                rows.append(row)

    # same deposit_id can be delivered more than once across daily files
    # (confirmed: VDEP002, VDEP005 both appear on 0301 and 0302) -- keep the
    # most recently delivered version.
    latest_by_id = {}
    for row in rows:
        existing = latest_by_id.get(row["deposit_id"])
        if existing is None or row["source_file"] > existing["source_file"]:
            latest_by_id[row["deposit_id"]] = row
    return list(latest_by_id.values())


def load_warehouse_deposits(path: Path) -> list:
    records = json.loads(path.read_text())
    for r in records:
        r["amount_usd"] = float(r["amount_usd"])
        # DEP012 was captured with a malformed column (`credit_card` instead
        # of `payment_method`) -- recover the value rather than dropping it.
        if r.get("payment_method") is None and "credit_card" in r:
            r["payment_method"] = r["credit_card"]
    return records


def load_known_client_ids(signup_path: Path) -> set:
    records = json.loads(signup_path.read_text())
    return {r["client_id"] for r in records}


def business_key(row: dict) -> tuple:
    return tuple(row.get(k) for k in BUSINESS_KEY)


def reconcile(vendor_rows: list, warehouse_rows: list, known_clients: set) -> list:
    warehouse_by_key = {}
    for row in warehouse_rows:
        warehouse_by_key.setdefault(business_key(row), row)

    matched_warehouse_keys = set()
    report = []

    for vrow in vendor_rows:
        key = business_key(vrow)
        wrow = warehouse_by_key.get(key)
        issues = []

        if vrow["amount_usd"] < 0:
            issues.append("negative_amount")
        if vrow["client_id"] not in known_clients:
            issues.append("unknown_client_id")

        if wrow is None:
            status = "vendor_only"
        else:
            matched_warehouse_keys.add(key)
            diffs = [f for f in COMPARE_FIELDS if str(vrow.get(f)) != str(wrow.get(f))]
            status = "mismatch" if diffs else "matched"
            if diffs:
                issues.append("field_diff:" + ",".join(diffs))

        report.append({
            "deposit_id": vrow["deposit_id"],
            "client_id": vrow["client_id"],
            "deposit_date": vrow["deposit_date"],
            "amount_usd": vrow["amount_usd"],
            "reconciliation_status": status,
            "issues": ";".join(issues) if issues else "",
            "source_file": vrow["source_file"],
        })

    for key, wrow in warehouse_by_key.items():
        if key not in matched_warehouse_keys:
            report.append({
                "deposit_id": wrow["deposit_id"],
                "client_id": wrow["client_id"],
                "deposit_date": wrow["deposit_date"],
                "amount_usd": wrow["amount_usd"],
                "reconciliation_status": "warehouse_only",
                "issues": "",
                "source_file": "warehouse",
            })

    return report


def write_report(report: list, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["deposit_id", "client_id", "deposit_date", "amount_usd",
                  "reconciliation_status", "issues", "source_file"]
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vendor-glob", default="data/DEPOSITS_VENDOR_*.CSV")
    parser.add_argument("--warehouse", type=Path, default=Path("data/CLIENT_DEPOSIT.JSON"))
    parser.add_argument("--signup", type=Path, default=Path("data/CLIENT_SIGNUP.JSON"))
    parser.add_argument("--out", type=Path, default=Path("code/scripts/output/reconciliation_report.csv"))
    args = parser.parse_args()

    vendor_rows = load_vendor_deposits(args.vendor_glob)
    warehouse_rows = load_warehouse_deposits(args.warehouse)
    known_clients = load_known_client_ids(args.signup)

    report = reconcile(vendor_rows, warehouse_rows, known_clients)
    write_report(report, args.out)

    counts = {}
    for row in report:
        counts[row["reconciliation_status"]] = counts.get(row["reconciliation_status"], 0) + 1

    print(f"{len(vendor_rows)} deduped vendor deposits, {len(warehouse_rows)} warehouse deposits")
    print("Reconciliation summary:", counts)
    flagged = [r for r in report if r["issues"]]
    print(f"{len(flagged)} row(s) flagged with data quality issues")


if __name__ == "__main__":
    main()
