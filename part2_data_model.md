# Part 2 — Data Model & Historization

## 2a. Dimensional Model / ERD

**Approach: Kimball star schema.**

Chosen over Data Vault because the dataset is a small number of well-understood, stable entities (clients, deposits, trades) with a clear conformed grain and a direct BI/reporting consumer (the weekly C-suite report from Part 3). A star schema keeps SCD2 history directly queryable via a single dimension join, without needing a hub/satellite virtualization layer to reconstruct point-in-time state — that overhead is justified for many-source, high-change-of-structure environments, which this isn't.

### Dimensions

**`dim_client`** (SCD Type 2 — see 2b for full mechanics)
- `client_key` (surrogate PK), `client_id` (natural key)
- Static attributes (from `client_signup`): `signup_date`, `country`, `referral_source`, `account_type`, `signup_platform`, `promo_code`, `assigned_manager`, `kyc_status`
- CDC-tracked attributes (from `client_profile` + `client_profile_changes`): `risk_category`, `account_balance_usd`, `account_status`
- Other profile attributes: `full_name`, `date_of_birth`, `nationality`, `currency`, `preferred_language`, `last_login_date`
- Versioning: `effective_from`, `effective_to`, `is_current`, `is_deleted`
- Audit: `source_lsn` (last applied lsn), `is_inferred` (true for early-arriving-fact placeholder rows — see below)

**`dim_date`** — standard calendar dimension for `deposit_date` / `trade_date` joins (`date_key`, `date`, `year`, `month`, `day`, `is_weekend`, …)

**`dim_instrument`** — for trades: `instrument_key`, `instrument_name` (`EUR/USD`, `Gold`, `BTC/USD`, `S&P500`, `USD/JPY`), `asset_class` (forex / commodity / crypto / index)

**`dim_payment_method`** — optional. Only 3 distinct values in the data (`bank_transfer`, `credit_card`, `e_wallet`); given the low cardinality I'd keep this as a degenerate attribute directly on `fact_deposit` rather than a separate join.

### Facts

**`fact_deposit`** — grain: **one row per deposit** (a single client-facing deposit transaction), sourced from both warehouse-native deposits and the reconciled vendor feed
- `deposit_key` (surrogate), `client_key` (FK → `dim_client`, resolved as-of `deposit_date`), `date_key`, `payment_method`
- Measures: `amount_usd`, `exchange_rate`, `fee_usd`, `processing_days`, `currency_original`
- `source_system` (`warehouse` / `vendor`), `source_deposit_id` (native `DEP*` or `VDEP*`), `reconciliation_status` (`matched` / `vendor_only` / `warehouse_only` / `mismatch`)

**`fact_trade`** — grain: **one row per trade**
- `trade_key`, `client_key` (FK, resolved as-of `trade_date`), `date_key`, `instrument_key`
- `direction`, `volume_lots`, `open_price`, `close_price`, `pnl_usd`, `trade_status`

See [sql/schema.sql](sql/schema.sql) for DDL.

### Late-Arriving Dimension Handling

Directly evidenced in the provided data, not hypothetical: `CL031` has a deposit in `client_deposit.json` but no matching row in `client_signup` or `client_profile` anywhere; `CL099` shows up the same way in the vendor feed. Textbook early-arriving-fact scenario.

**Handling — early-arriving-fact / inferred-member pattern:**
1. When a fact load encounters a `client_id` with no matching `dim_client` row, insert a placeholder: new `client_key` generated, `client_id = CL031`, `is_inferred = true`, descriptive attributes null/unknown, `is_current = true`.
2. The fact loads normally against this placeholder `client_key` — no fact data is ever blocked or dropped waiting on dimension completeness.
3. When the real signup/profile record (or a CDC insert) eventually arrives for `CL031`, it does **not** create a second `dim_client` row. It updates the existing inferred row in place if this is its first real version, or closes it out via normal SCD2 versioning if attributes differ from the placeholder's defaults, flipping `is_inferred` to `false`.
4. Facts already loaded against the placeholder's `client_key` don't need to be touched or reprocessed — the key is stable across enrichment, so existing `fact_deposit` / `fact_trade` rows automatically inherit the enriched dimension data through the join.

---

## 2b. Historization (SCD)

### 1. SCD type: Type 2 for `risk_category`, `account_balance_usd`, `account_status`

**Justification**: the business needs point-in-time history for these fields — e.g. answering "what was this client's `risk_category` on date X" for a compliance or fraud review, which Type 1 (overwrite) destroys outright. Type 3 (limited previous-value columns) can't support an unbounded number of changes — `CL001` alone has 3 recorded changes in the sample CDC feed. Type 2 versioned rows with `effective_from` / `effective_to` give exact point-in-time reconstruction and map directly onto the CDC feed's own before/after event shape.

**Trade-off**: `dim_client` fans out to multiple rows per `client_id` rather than staying 1:1, so every downstream join must resolve "the version active as of the fact's date" (via `effective_from`/`effective_to`, not a plain lookup by `client_id`) — more ETL complexity and a larger dimension table, in exchange for full auditability.

### 2. Update / delete merge logic

**Staging**: land CDC JSONL append-only and immutable; sort each batch by (`client_id`, `lsn`) ascending. This ordering step is load-bearing, not optional — see Part 1's edge case 7. Arrival order in the file does not match `lsn` order: `CL001`'s `lsn 1005` arrives before `lsn 1004`, but `1005`'s `before.risk_category` value is the direct *result* of what `1004` produces — applying arrival order would process `1005` against a `before` state that hasn't happened yet. Same bug shape for `CL014`'s `lsn 1009` vs `1008`.

**Merge, per `client_id`, replaying events in `lsn` order**:
- `insert` → create the first current row (`effective_from = commit_ts`, `is_current = true`)
- `update` → close the existing current row (`effective_to = commit_ts`, `is_current = false`), insert a new current row with the `after` values
- `delete` → close the existing current row, insert a new terminal current row with `is_deleted = true`, attributes copied from the `before` payload, `effective_to = null`

**Idempotency**: a `processed_events` table keyed on (`client_id`, `lsn`) records what's already been applied; reprocessing an already-seen `lsn` for a client is a no-op.

**Walkthrough — `CL012`'s delete (`lsn 1010`)**:
`CL012` has exactly one CDC event in the sample, a delete, with no competing update. Processing: close the client's original `dim_client` row (from the initial profile load) at `effective_to = 2024-11-21T14:00:00Z`, `is_current = false`. Insert a new terminal row: `is_deleted = true`, `is_current = true`, `effective_to = null`, attributes taken from the `before` payload (`full_name: David Tan`, `risk_category: low`, `account_balance_usd: 0.00`, `account_status: suspended`). Any fact rows already loaded against `CL012`'s earlier `client_key` are untouched. A query filtering `is_current = true AND is_deleted = false` correctly excludes `CL012` going forward, while an unfiltered or point-in-time query still sees the full lineage.

### 3. Reloading a historical date range (e.g. reprocess November 2024) without corrupting history

1. Identify affected landing batches by `ingestion_ts` / `commit_ts` falling inside the target range.
2. Re-stage those batches using the exact same deterministic logic as the original run (parse → dedupe → sort by `client_id, lsn`). If nothing about the underlying data or transform logic changed, this is a true no-op against `processed_events` — nothing new gets written.
3. If the reload is a genuine correction (e.g. a bug in the original transform), **never delete or overwrite existing SCD2 rows in place.** Instead: tag the affected rows with a `reload_batch_id` for audit traceability, insert corrected versions with their own `effective_from`/`effective_to`, and only repoint `is_current` at the actual current boundary. The superseded rows stay intact for audit.
4. Never truncate-and-reload the whole `dim_client` table for a partial-range fix — always scope to the affected `client_id`s and date range, and reconcile via the same versioned-insert mechanism, so unaffected clients' history is never touched.
