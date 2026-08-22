-- Dimensional model DDL for the trading warehouse, as designed in
-- part2_data_model.md section 2a. DuckDB syntax (matches the code/dbt
-- prototype); translates directly to any standard SQL warehouse.
--
-- This is the target schema dim_clients.sql / fct_trading_performace.sql
-- build into via dbt -- this file documents the model on its own, it is not
-- executed by the dbt project (dbt creates these tables itself from the
-- model SQL + _staging.yml / _marts.yml contracts).

create schema if not exists marts;

-- ============================================================================
-- DIMENSIONS
-- ============================================================================

-- SCD Type 2 on risk_category, account_balance_usd, account_status.
-- One row per (client_id, version) -- NOT one row per client. See
-- part2_data_model.md section 2b for the full replay/merge logic.
create table marts.dim_client (
    client_key           varchar primary key,   -- surrogate key: hash(client_id, effective_from)
    client_id             varchar not null,      -- natural key, repeats across versions

    -- static attributes, from client_signup
    signup_date           date,
    country                varchar,
    email                  varchar,
    referral_source        varchar,
    account_type           varchar,
    kyc_status             varchar,
    signup_platform        varchar,
    promo_code             varchar,
    assigned_manager       varchar,

    -- CDC-tracked attributes -- risk_category / account_balance_usd /
    -- account_status are the three SCD2 versions on; the rest ride along
    full_name              varchar,
    date_of_birth          date,
    nationality            varchar,
    risk_category          varchar,
    account_balance_usd    decimal(18,2),
    account_status         varchar,
    currency               varchar,
    preferred_language     varchar,
    last_login_date        date,

    -- SCD2 versioning
    effective_from         timestamp not null,
    effective_to           timestamp,              -- null = still open
    is_current              boolean not null,
    is_deleted               boolean not null default false,

    -- audit / provenance
    source_lsn                bigint,               -- last CDC lsn applied to produce this version; null for the pre-CDC baseline row
    is_inferred                boolean not null default false  -- true for early-arriving-fact placeholder rows (see part2_data_model.md "Late-Arriving Dimension Handling")
);

create index idx_dim_client_natural_key on marts.dim_client (client_id, effective_from);
-- Exactly one is_current = true row per client_id at all times -- enforced
-- in the dbt project via tests/assert_one_current_version_per_client.sql
-- (a real bug in the CDC replay was caught by this test -- see PROMPTS.md).

-- Standard calendar dimension for deposit_date / trade_date joins.
create table marts.dim_date (
    date_key       integer primary key,   -- yyyymmdd
    date            date not null,
    year             integer not null,
    month             integer not null,
    day                integer not null,
    day_of_week         integer not null,
    is_weekend           boolean not null
);

-- One row per distinct traded instrument.
create table marts.dim_instrument (
    instrument_key    varchar primary key,
    instrument_name    varchar not null,   -- e.g. 'EUR/USD', 'Gold', 'BTC/USD'
    asset_class          varchar not null   -- forex / commodity / crypto / index
);

-- dim_payment_method deliberately omitted: only 3 distinct values
-- (bank_transfer, credit_card, e_wallet) in the source data -- kept as a
-- degenerate attribute directly on fact_deposit instead of a separate join
-- (see part2_data_model.md section 2a).

-- ============================================================================
-- FACTS
-- ============================================================================

-- Grain: one row per deposit (client-facing deposit transaction), sourced
-- from both the existing warehouse deposits and the reconciled vendor feed.
create table marts.fact_deposit (
    deposit_key             varchar primary key,
    client_key               varchar not null references marts.dim_client (client_key),  -- resolved as-of deposit_date, not a plain client_id lookup
    date_key                   integer not null references marts.dim_date (date_key),

    payment_method               varchar,   -- degenerate dimension, see note above

    amount_usd                     decimal(18,2) not null,
    exchange_rate                    decimal(18,6),
    fee_usd                           decimal(18,2),
    processing_days                    integer,
    currency_original                    varchar,

    source_system                          varchar not null,   -- 'warehouse' | 'vendor'
    source_deposit_id                       varchar not null,   -- native DEP* or VDEP* id -- these are disjoint namespaces, see part1_pipeline.md
    reconciliation_status                     varchar             -- 'matched' | 'vendor_only' | 'warehouse_only' | 'mismatch'
);

-- Grain: one row per trade.
create table marts.fact_trade (
    trade_key         varchar primary key,
    client_key          varchar not null references marts.dim_client (client_key),  -- resolved as-of trade_date
    date_key               integer not null references marts.dim_date (date_key),
    instrument_key           varchar not null references marts.dim_instrument (instrument_key),

    direction                  varchar not null,
    volume_lots                  decimal(18,4),
    open_price                     decimal(18,6),
    close_price                      decimal(18,6),
    pnl_usd                            decimal(18,2),
    trade_status                         varchar
);

-- ============================================================================
-- SUPPORTING: CDC idempotency (see part2_data_model.md section 2b, "Idempotency")
-- ============================================================================

-- Tracks which (client_id, lsn) CDC events have already been applied to
-- dim_client, so replaying an already-seen event is a no-op rather than a
-- duplicate version.
create table marts.processed_events (
    client_id       varchar not null,
    lsn                bigint not null,
    processed_at         timestamp not null default current_timestamp,
    primary key (client_id, lsn)
);
