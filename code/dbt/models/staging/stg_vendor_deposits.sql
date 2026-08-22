-- Staging: vendor deposit feed, unioned across daily deliveries.
--
-- Handles:
--   - schema drift: `payment_method` was renamed to `method` in the
--     2024-03-02 delivery only.
--   - cross-batch duplicate delivery: the same deposit_id can legitimately
--     reappear across daily files (confirmed: VDEP002 and VDEP005 are both
--     delivered on 0301 and again on 0302) -- kept as the most recently
--     *delivered* version, so a corrected re-delivery wins over the original.
--
-- Sources: raw.deposits_vendor_20240301 / _20240302 / _20240303
--          (loaded from DEPOSITS_VENDOR_YYYYMMDD.CSV)
--
-- Note: this model does NOT join against dim_clients or the warehouse deposit
-- table -- cross-system reconciliation (vendor VDEP* vs warehouse DEP*, which
-- are disjoint ID namespaces) is handled separately in
-- scripts/deposit_reconciliation.py, since it's a business-key match, not a
-- staging-layer concern.

with vendor_20240301 as (

    select
        deposit_id,
        client_id,
        deposit_date,
        amount_usd,
        payment_method,
        currency_original,
        exchange_rate,
        status,
        processing_days,
        fee_usd,
        date '2024-03-01' as source_file_date

    from {{ source('raw', 'deposits_vendor_20240301') }}

),

vendor_20240302 as (

    select
        deposit_id,
        client_id,
        deposit_date,
        amount_usd,
        method            as payment_method,
        currency_original,
        exchange_rate,
        status,
        processing_days,
        fee_usd,
        date '2024-03-02' as source_file_date

    from {{ source('raw', 'deposits_vendor_20240302') }}

),

vendor_20240303 as (

    select
        deposit_id,
        client_id,
        deposit_date,
        amount_usd,
        payment_method,
        currency_original,
        exchange_rate,
        status,
        processing_days,
        fee_usd,
        date '2024-03-03' as source_file_date

    from {{ source('raw', 'deposits_vendor_20240303') }}

),

unioned as (

    select * from vendor_20240301
    union all
    select * from vendor_20240302
    union all
    select * from vendor_20240303

),

typed as (

    select
        deposit_id,
        client_id,
        cast(deposit_date as date)           as deposit_date,
        cast(amount_usd as decimal(18,2))    as amount_usd,
        payment_method,
        currency_original,
        cast(exchange_rate as decimal(18,6)) as exchange_rate,
        status,
        cast(processing_days as integer)     as processing_days,
        cast(fee_usd as decimal(18,2))       as fee_usd,
        source_file_date,

        -- data quality flag -- routed to quarantine downstream, not filtered
        -- out here (e.g. VDEP001 = -250.00).
        case when amount_usd < 0 then true else false end as is_negative_amount

    from unioned

),

deduped as (

    select
        deposit_id, client_id, deposit_date, amount_usd, payment_method,
        currency_original, exchange_rate, status, processing_days, fee_usd,
        source_file_date, is_negative_amount
    from (
        select
            typed.*,
            row_number() over (
                partition by deposit_id
                order by source_file_date desc
            ) as rn
        from typed
    ) ranked
    where rn = 1

)

select * from deduped
