# Part 1 — Pipeline Design & Reconciliation

## 1. Architecture Overview

Medallion architecture (Landing → Staging → Target). The vendor CSV feed and the CDC log are handled as two separate arms through the same three layers, because they have fundamentally different delivery semantics — one is a daily batch snapshot, the other is an ordered change stream.

### Arm A — Vendor deposit feed (CSV)

- **Landing**: raw CSV files landed byte-for-byte, immutable, one object per delivery. Each file tagged with `load_id`, `source_filename`, `ingestion_ts`. No transformation at this layer.
- **Staging**:
  - Schema contract enforcement — map source columns to the canonical schema explicitly (not positionally), so a rename like `payment_method` → `method` is caught as a contract violation rather than silently misaligned.
  - Type validation (`amount_usd` numeric, `deposit_date` valid date), whitespace/trim normalization.
  - Data quality checks routed to quarantine with severity (see table below).
  - Dedupe within/across batches on the vendor's own `deposit_id`.
- **Target**:
  - Vendor rows UPSERT into `fact_deposit` keyed on the vendor's `deposit_id` (source-tagged, see Idempotency below).
  - A separate **reconciliation step** compares staged vendor rows against existing warehouse deposits on a business key (`client_id`, `deposit_date`, `amount_usd`) and classifies each as `matched` / `vendor_only` / `warehouse_only` / `mismatch`, written to a reconciliation report table.

### Arm B — CDC change log (JSONL)

- **Landing**: JSONL landed append-only, immutable, arrival order preserved exactly as received (this is *not* the processing order — see below).
- **Staging**:
  - Parse and validate required fields (`lsn`, `op`, `client_id`).
  - Dedupe exact re-deliveries (same `lsn` + `client_id` + `op` seen twice).
  - **Re-sequence: sort by (`client_id`, `lsn`) ascending.** This step is load-bearing — arrival order does not match `lsn` order, and applying events out of sequence produces incorrect intermediate state (concrete examples under Edge Cases below).
- **Target**: events replayed in `lsn` order per client into `dim_client` (SCD Type 2) — insert/update creates a new version, delete closes the current row and inserts a soft-deleted terminal row. Full mechanics in Part 2b.

---

## 2. Idempotency Strategy

Two distinct problems, two distinct mechanisms — don't conflate them:

**a) File/batch-level idempotency** — "has this file already been processed?"
A manifest table keyed on (`source_system`, `filename`, `file_checksum`). Before landing, check the checksum; if already present, skip. Protects against accidental re-drop of an identical file.

**b) Record-level idempotency (within a single source)**
- Vendor deposits: `MERGE`/`UPSERT` keyed on the vendor's own `deposit_id`. This is necessary and sufficient — confirmed by the data, since `VDEP002` and `VDEP005` appear identically in both `20240301` and `20240302`; a keyed merge naturally no-ops on the repeat.
- CDC events: `MERGE` keyed on (`client_id`, `lsn`) tracked in a `processed_events` audit table. Reapplying an already-applied `lsn` for a client is a no-op.

**c) Cross-system reconciliation key — a separate concern from load idempotency**
The vendor's `deposit_id` (`VDEP*`) and the warehouse's existing `deposit_id` (`DEP*`) are **disjoint ID namespaces** — checked every warehouse deposit against every vendor row and there is zero ID overlap. This means "reconciling the vendor feed against the warehouse" cannot be an ID join. It has to be a **business-key match** on (`client_id`, `deposit_date`, `amount_usd`) to determine whether a vendor-reported deposit is already reflected in the warehouse (possibly under a different ID), is genuinely new, or conflicts in value with what's already loaded.

---

## 3. Late and Missing Data

Two separate mechanisms — a single "watermark" isn't the right framing given the load is keyed-merge, not date-partitioned:

**Completeness / SLA monitoring** — detects *"we never received a file"*:
An expected-file calendar (one `deposits_vendor_YYYYMMDD.csv` per business day) is compared against received filenames/dates on a schedule; a gap beyond SLA (e.g. no file by T+4h) pages on-call. This is the only piece that needs active monitoring.

**Late-record self-reconciliation** — detects *"the file arrived late or contains old dates, does it still load correctly"*:
Because loading is a keyed `MERGE` on `deposit_id` rather than a date-partitioned overwrite, a late or back-dated record self-heals the moment the pipeline next runs — no manual backfill needed. Confirmed directly by `20240303.csv`: it's delivered on day 3 but every row's `deposit_date` falls between Feb 24–28, predating even day 1. No special-case logic is required — the row simply upserts by `deposit_id` regardless of how "old" its business date is.

---

## 4. Source-Delete Handling

CDC delete events never hard-delete from the warehouse:

1. Close the client's current SCD2 row (`effective_to = commit_ts`, `is_current = false`).
2. Insert a new terminal row (`is_deleted = true`, `is_current = true`, `effective_to = null`) carrying the last-known attribute values from the event's `before` payload — preserving a full audit trail rather than just flipping a flag on the existing row in place.

Downstream consumers filter `is_current = true AND is_deleted = false` for "active clients"; audit and point-in-time queries retain the complete lineage since no row is ever physically removed.

**Trade-off**: `dim_client` grows monotonically (never shrinks) and every downstream query needs the `is_deleted` filter. Accepted deliberately — the constraint explicitly disallows hard deletes, and the compliance/audit value of retained history outweighs the storage and query-filter cost.

---

## 5. Edge Cases

| # | Edge case | Evidence in provided data | Handling strategy |
|---|---|---|---|
| 1 | Cross-batch duplicate delivery | `VDEP002`, `VDEP005` appear identically in both `20240301.csv` and `20240302.csv` | Keyed `MERGE` on `deposit_id` — re-delivery is a natural no-op |
| 2 | Schema drift | `20240302.csv` renames `payment_method` → `method` | Staging applies an explicit column-mapping contract per source/date; an unmapped/unexpected column is a HIGH-severity halt, not a silent null-fill |
| 3 | Late file with back-dated records | `20240303.csv` delivered day 3, all `deposit_date` values predate day 1 | Keyed merge self-heals automatically (see §3); SLA monitor separately tracks on-time *delivery* |
| 4 | Unknown/orphan `client_id` | `CL099` (vendor row `VDEP020`) and `CL031` (existing `client_deposit` table) have no matching signup/profile row | Load against an inferred placeholder `dim_client` row (early-arriving-fact pattern — detailed in Part 2a) rather than rejecting the fact |
| 5 | Malformed source record | `DEP012` in the existing `client_deposit` table has a `credit_card` key instead of `payment_method` | Schema validation on load — MEDIUM severity, quarantine with raw payload preserved for manual review rather than guessing the intended field |
| 6 | Negative/implausible amount | `VDEP001`: `amount_usd = -250.00` | HIGH severity, quarantine + alert — not silently `abs()`'d or dropped, since it may indicate either a vendor error or a legitimate reversal needing separate handling |
| 7 | CDC arrival order ≠ `lsn` order | `CL001`'s `lsn 1005` arrives before `lsn 1004` in the file, but `1005`'s `before.risk_category` value is the *result* of `1004`; same pattern for `CL014`'s `lsn 1009` vs `1008` | Staging always sorts by (`client_id`, `lsn`) ascending before merge — never apply in raw arrival order |

---

## Data Quality Safeguards (optional, included)

| Check | Severity | On-failure action |
|---|---|---|
| Negative `amount_usd` | High | Quarantine row, page on-call, exclude from load |
| Unmapped/unexpected CSV column (schema drift) | High | Halt load for that file, alert data engineering, require explicit contract update before reprocessing |
| `client_id` not found in any client dimension source | Medium | Load under inferred/placeholder `dim_client` row, flag for backfill reconciliation |
| Missing/null required field (`deposit_id`, `client_id`, `amount_usd`) | High | Quarantine row, do not load |
| `deposit_date` more than N days outside the file's delivery date | Low | Log and load — informational only, given the known late-delivery pattern in this feed; no blocking action |
