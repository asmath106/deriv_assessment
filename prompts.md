# AI Prompts Used

All work in this repo was done with Claude (Claude Code) as a collaborator — reviewing my own draft design, grounding it against the actual data files, and then implementing/validating the code once the design was settled. Entries below are grouped by part, in the order they happened, paraphrased from the actual session.

---

## Part 1 — Pipeline Design & Reconciliation

**Prompt 1:**
> "This is my approach: medallion architecture — landing layer for vendor CSVs, landed raw and immutable with event and ingestion timestamp; staging validates data types, invalid records, whitespace, schema contract, deduplication; target dimension layer will be SCD Type 2 and Kimball. Idempotency: MERGE/upsert, using a primary key like `md5(account_id, transaction_date, amount)`. Late/missing data: SLA monitors, dynamic event-time watermarks with a rolling window. Source-delete handling: soft deletes combined with SCD Type 2, `is_deleted = true`. Review my architecture and suggest [improvements]."

What I changed based on the output: Claude flagged that the `md5(account_id + transaction_date + amount)` composite key would silently collide two *legitimate* same-day, same-amount deposits from the same client — a real correctness bug, not a style nitpick. It pointed out the vendor CSV already ships a stable natural key (`deposit_id`) and recommended merging on that directly instead of hashing. It also flagged that my "watermark + rolling window" framing didn't actually match a keyed-merge load (which self-heals late records without any windowing logic), and that I hadn't addressed CDC event ordering at all, even though the source data explicitly warns arrival order ≠ `lsn` order. I accepted the `deposit_id`-based merge key and dropped the watermark/rolling-window idea in favor of a plain SLA/completeness check; I had not previously realized the CDC ordering issue was a real risk in this specific dataset until this pass.

**Prompt 2:**
> "Inspect the data and compare with my approach."

What I changed based on the output: this is where the design meaningfully deepened rather than just got critiqued. Claude checked every vendor deposit row against every existing warehouse deposit row and found the vendor's `deposit_id` (`VDEP*`) and the warehouse's `deposit_id` (`DEP*`) are **disjoint ID namespaces** with zero overlap in the sample data — which meant my Prompt-1 fix (merge on `deposit_id`) was actually incomplete: it solves within-feed idempotency but can't be the reconciliation key, since there's no shared ID space to reconcile against. I revised the design to two separate mechanisms (within-feed dedup on the vendor's own ID vs. cross-system reconciliation on a business key) based on this finding, rather than the single merge key I'd originally proposed. Claude also traced two concrete broken CDC sequences in the actual data (`CL001`'s `lsn 1005` depends on `lsn 1004`'s effect but arrives before it in the file; same pattern for `CL014`'s `lsn 1009`/`1008`) — I used these as the worked examples in the final design doc instead of an abstract statement about sort order.

**Prompt 3 (code):**
> "Complete the DAG with task 1: process and validate the CDC changelog with lsn ordering; task 2: ingest and reconcile vendor deposits against warehouse; task 3: run dbt transformation (staging and marts) with data contract tests. Run dbt tests for data quality and SLAs. Potential data quality issues found: client_id with 99 in the vendor deposit file on the 3rd, amount_usd of -250 on the first deposit file, a client's date of birth is 1888 — validate and accommodate them in the tests."

What I changed based on the output: Claude turned each of these three findings into an actual dbt test (not a comment) with a deliberately different severity per case — `error` for the negative amount (matches the HIGH severity already decided in Part 1's own data quality table) vs. `warn` for the unknown client and the bad DOB (both are cases the design already treats as expected/self-healing, so a hard failure would be wrong). I accepted this severity split as-is — it's consistent with the design rather than a blanket "fail on everything" policy.

---

## Part 2 — Data Model & Historization

**Prompt 4:**
> "Update the three md files with the approach for part 1, part 2, and part 3, based on the findings."

I was explicit that I didn't want the assessment written *for* me — Claude asked first whether I wanted a full prose draft, a bullet skeleton, or no file writes at all, and I chose the full-draft option since the actual decisions (SCD2, Kimball, the two-mechanism idempotency split, the `lsn`-ordering fix) were already mine from Prompts 1–2; this was capturing decisions already made, not delegating new ones. One thing I noted from the output: Claude declined to write anything substantive into Part 3, since we hadn't actually discussed a real-time/batch architecture or a build-vs-buy position yet — it only scaffolded the required headers rather than inventing an answer. I agreed with holding off there.

**Prompt 5 (code):**
> "My approach for dbt models: under staging — `stg_client_signup.sql`, `stg_client_profile.sql`, `stg_vendor_deposits.sql`, `stg_clients_deposit.sql`; under marts — `dim_clients.sql`, `fct_trading_performace.sql`; under scripts — `cdc_processor.py`, `deposit_reconciliation.py`. Write code for all the files according to your understanding, I will review at the end."

What I changed based on the output: I deferred the actual SQL/Python authorship to Claude here and reviewed the result rather than writing it myself. The late-arriving-dimension handling in `dim_clients.sql` (the inferred-placeholder pattern) was built directly off the `CL031`/`CL099` orphan-client finding from Part 1's Prompt 2 — I confirmed this was the right anchor rather than a hypothetical example, since the assessment specifically asks for late-arriving-dimension handling to be addressed.

**Prompt 6 (validation):**
> "Add the [landing] loader and install dbt/duckdb to validate end to end, and update the README with detailed steps on my approach, design files created, why, tests created, an architecture diagram, and a detailed end-to-end flow."

What I changed based on the output — this is the one place I'd flag as a genuine AI mistake, not just a refinement: running the pipeline for real (not just reading the SQL) surfaced an actual bug. The singular test `assert_one_current_version_per_client` failed: client `CL030` ended up with two simultaneous `is_current = true` rows in `dim_clients`. Cause: `CL030` exists in the baseline `CLIENT_PROFILE.JSON` snapshot **and** has a separate `insert` event in the CDC log (`lsn 1001`); the `cdc_processor.py` Claude had written in Prompt 5 treated every `insert` op as a brand-new genesis version without checking whether the client already had an open version, so neither branch ever got closed. This was a real logic defect in AI-generated code that I would not have caught from reading the SQL/Python alone — it only surfaced because the design's own SCD2 integrity test was run against real data. Claude fixed the root cause (an `insert` for a client that already has an open version now behaves like an `update`: close the old version, or no-op if the attributes are identical) rather than deleting or weakening the test, then re-ran the full pipeline to confirm. I'm keeping the test in the repo specifically because it did its job.

---

## Part 3 — TL Extension

Not yet started. `part3_architecture.md` currently only has the brief's required section headers scaffolded — no prompts have been used for this part's actual content yet, since no design decisions have been made.
