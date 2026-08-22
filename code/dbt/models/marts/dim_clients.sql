-- Mart: client dimension, SCD Type 2 on risk_category / account_balance_usd /
-- account_status.
--
-- The CDC replay itself (lsn-ordering, insert/update/delete -> versioned
-- rows) happens upstream in scripts/cdc_processor.py, not here -- that's a
-- row-by-row sequential state replay, which SQL/dbt set-based transforms are
-- a poor fit for. This model assembles the dimension from:
--   1. stg_client_signup       -- static attributes, one row per client
--   2. the client_profile_history seed -- cdc_processor.py output: full SCD2
--      version history (baseline + every replayed CDC event, already sorted
--      and merged in (client_id, lsn) order). This is a dbt seed (a CSV
--      checked into code/dbt/seeds/), referenced via ref(), not source() --
--      it isn't an externally landed raw table.
--   3. placeholder rows for clients referenced by a fact before any
--      dimension record exists for them (early-arriving-fact pattern --
--      evidenced by CL031 in the existing deposit table and CL099 in the
--      vendor feed, neither of which has a signup/profile row)
--
-- Requires the dbt_utils package (packages.yml) for generate_surrogate_key.

with signup as (

    select * from {{ ref('stg_client_signup') }}

),

profile_history as (

    select
        client_id,
        full_name,
        date_of_birth,
        nationality,
        risk_category,
        account_balance_usd,
        account_status,
        currency,
        preferred_language,
        last_login_date,
        cast(effective_from as timestamp) as effective_from,
        cast(effective_to as timestamp)   as effective_to,
        is_current,
        is_deleted,
        source_lsn,
        false                             as is_inferred

    from {{ ref('client_profile_history') }}

),

known_client_ids as (

    select client_id from signup
    union
    select client_id from profile_history

),

orphan_client_ids as (

    select client_id from {{ ref('stg_vendor_deposits') }}
    union
    select client_id from {{ ref('stg_clients_deposit') }}

),

orphans as (

    select distinct o.client_id
    from orphan_client_ids o
    left join known_client_ids k on o.client_id = k.client_id
    where k.client_id is null

),

placeholder_versions as (

    select
        client_id,
        cast(null as varchar)       as full_name,
        cast(null as date)          as date_of_birth,
        cast(null as varchar)       as nationality,
        cast(null as varchar)       as risk_category,
        cast(null as decimal(18,2)) as account_balance_usd,
        cast(null as varchar)       as account_status,
        cast(null as varchar)       as currency,
        cast(null as varchar)       as preferred_language,
        cast(null as date)          as last_login_date,
        current_timestamp           as effective_from,
        cast(null as timestamp)     as effective_to,
        true                        as is_current,
        false                       as is_deleted,
        cast(null as bigint)        as source_lsn,
        true                        as is_inferred

    from orphans

),

all_versions as (

    select * from profile_history
    union all
    select * from placeholder_versions

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['v.client_id', 'v.effective_from']) }} as client_key,
        v.client_id,
        s.signup_date,
        s.country,
        s.email,
        s.referral_source,
        s.account_type,
        s.kyc_status,
        s.signup_platform,
        s.promo_code,
        s.assigned_manager,
        v.full_name,
        v.date_of_birth,
        v.nationality,
        v.risk_category,
        v.account_balance_usd,
        v.account_status,
        v.currency,
        v.preferred_language,
        v.last_login_date,
        v.effective_from,
        v.effective_to,
        v.is_current,
        v.is_deleted,
        v.is_inferred,
        v.source_lsn

    from all_versions v
    left join signup s on v.client_id = s.client_id

)

select * from final
