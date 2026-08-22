"""
Replays the client_profile CDC log (CLIENT_PROFILE_CHANGES.JSONL) against the
baseline CLIENT_PROFILE.JSON snapshot to produce a full SCD Type 2 version
history, consumed downstream by dbt as raw.client_profile_history.

Critical fix: the CDC file is delivered in arrival order, which does not
match lsn order. Verified in the data: CL001's lsn 1005 arrives before lsn
1004, but 1005's `before.risk_category` is the direct result of what 1004
produces -- replaying arrival order would apply 1005 against a state that
hasn't happened yet. Same pattern for CL014's lsn 1009 vs 1008. Events are
re-sorted by (client_id, lsn) before replay.

Idempotent by construction: this is a full deterministic recompute from
immutable inputs, so re-running with the same inputs always produces the
same output file.

Usage:
    python cdc_processor.py \\
        --profile data/CLIENT_PROFILE.JSON \\
        --changes data/CLIENT_PROFILE_CHANGES.JSONL \\
        --out code/dbt/seeds/client_profile_history.csv
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

PROFILE_ATTRS = [
    "full_name", "date_of_birth", "nationality", "risk_category",
    "account_balance_usd", "account_status", "currency",
    "preferred_language", "last_login_date",
]

BASELINE_EFFECTIVE_FROM = datetime(2024, 1, 1, tzinfo=timezone.utc)


@dataclass
class Version:
    client_id: str
    attrs: dict
    effective_from: datetime
    effective_to: Optional[datetime] = None
    is_current: bool = True
    is_deleted: bool = False
    source_lsn: Optional[int] = None


def load_baseline(path: Path) -> dict:
    records = json.loads(path.read_text())
    return {r["client_id"]: {k: r.get(k) for k in PROFILE_ATTRS} for r in records}


def load_events(path: Path) -> list:
    events = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    events.sort(key=lambda e: (e["client_id"], e["lsn"]))
    return events


def replay(baseline: dict, events: list) -> list:
    all_versions: list[Version] = []
    open_version: dict[str, Version] = {}

    for client_id, attrs in baseline.items():
        v = Version(client_id=client_id, attrs=dict(attrs), effective_from=BASELINE_EFFECTIVE_FROM)
        open_version[client_id] = v
        all_versions.append(v)

    for event in events:
        client_id = event["client_id"]
        op = event["op"]
        commit_ts = datetime.fromisoformat(event["commit_ts"].replace("Z", "+00:00"))
        lsn = event["lsn"]
        current = open_version.get(client_id)

        if op == "insert":
            new_attrs = {k: event["after"].get(k) for k in PROFILE_ATTRS}

            if current is not None:
                # This client already has an open version -- either from the
                # baseline snapshot or an earlier event -- so a second
                # "insert" can't be a genesis event. Found in the data:
                # CL030 has both a CLIENT_PROFILE.JSON baseline row and an
                # `insert` CDC event (lsn 1001), which without this check
                # left two simultaneous is_current=true rows for the same
                # client (caught by tests/assert_one_current_version_per_client.sql).
                # Treat it like an update: if the attrs are identical it's a
                # redundant re-assertion of the same state (skip), otherwise
                # close the existing version and open a new one.
                if current.attrs == new_attrs:
                    continue
                current.effective_to = commit_ts
                current.is_current = False

            new_version = Version(client_id=client_id, attrs=new_attrs, effective_from=commit_ts, source_lsn=lsn)
            open_version[client_id] = new_version
            all_versions.append(new_version)
            continue

        if current is None:
            # update/delete with no prior baseline or insert -- orphan CDC
            # event. Skip rather than fabricate history; a real pipeline
            # would route this to a quarantine sink for investigation.
            continue

        current.effective_to = commit_ts
        current.is_current = False

        if op == "update":
            merged_attrs = dict(current.attrs)
            merged_attrs.update({k: v for k, v in event["after"].items() if k in PROFILE_ATTRS})
            new_version = Version(client_id=client_id, attrs=merged_attrs, effective_from=commit_ts, source_lsn=lsn)
            open_version[client_id] = new_version
            all_versions.append(new_version)

        elif op == "delete":
            deleted_attrs = dict(current.attrs)
            deleted_attrs.update({k: v for k, v in (event.get("before") or {}).items() if k in PROFILE_ATTRS})
            terminal_version = Version(
                client_id=client_id, attrs=deleted_attrs, effective_from=commit_ts,
                source_lsn=lsn, is_deleted=True,
            )
            open_version[client_id] = terminal_version
            all_versions.append(terminal_version)

    return all_versions


def write_csv(versions: list, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["client_id"] + PROFILE_ATTRS + [
        "effective_from", "effective_to", "is_current", "is_deleted", "source_lsn",
    ]
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for v in versions:
            writer.writerow({
                "client_id": v.client_id,
                **v.attrs,
                "effective_from": v.effective_from.isoformat(),
                "effective_to": v.effective_to.isoformat() if v.effective_to else "",
                "is_current": v.is_current,
                "is_deleted": v.is_deleted,
                "source_lsn": v.source_lsn if v.source_lsn is not None else "",
            })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, default=Path("data/CLIENT_PROFILE.JSON"))
    parser.add_argument("--changes", type=Path, default=Path("data/CLIENT_PROFILE_CHANGES.JSONL"))
    parser.add_argument("--out", type=Path, default=Path("code/dbt/seeds/client_profile_history.csv"))
    args = parser.parse_args()

    baseline = load_baseline(args.profile)
    events = load_events(args.changes)
    versions = replay(baseline, events)
    write_csv(versions, args.out)

    print(f"{len(baseline)} clients loaded from baseline")
    print(f"{len(events)} CDC events replayed in (client_id, lsn) order")
    print(f"{len(versions)} total SCD2 versions written to {args.out}")
    deleted = [v for v in versions if v.is_deleted]
    print(f"{len(deleted)} client(s) soft-deleted: {[v.client_id for v in deleted]}")


if __name__ == "__main__":
    main()
