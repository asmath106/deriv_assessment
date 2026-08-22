-- Staging: existing warehouse deposit table (pre-vendor-feed baseline).
--
-- Handles: malformed source record DEP012, which was captured with a
-- `credit_card` key instead of `payment_method` in the raw JSON. Assumes a
-- schema-on-read JSON loader (e.g. DuckDB read_json with union_by_name) that
-- surfaces `credit_card` as its own column rather than dropping the row.
--
-- Source: raw.client_deposit (loaded from CLIENT_DEPOSIT.JSON)

with source as (

    select * from {{ source('raw', 'client_deposit') }}

),

typed as (

    select
        deposit_id,
        client_id,
        cast(deposit_date as date)            as deposit_date,
        cast(amount_usd as decimal(18,2))     as amount_usd,

        -- recover the value rather than drop the row -- the payment method
        -- itself ("credit_card") is still there, just under the wrong key.
        coalesce(payment_method, credit_card) as payment_method,

        currency_original,
        cast(exchange_rate as decimal(18,6))  as exchange_rate,
        status,
        cast(processing_days as integer)      as processing_days,
        cast(fee_usd as decimal(18,2))        as fee_usd,

        case
            when payment_method is null and credit_card is not null then true
            else false
        end                                     as had_malformed_schema

    from source

)

select * from typed
