-- Mart: one row per trade, joined to the client dimension version that was
-- active as of the trade date (SCD2 as-of join -- not a plain client_id
-- lookup, since dim_clients fans out to multiple versions per client).
--
-- Note: there is no stg_client_trades model in the current staging layer, so
-- this reads directly from the raw trades source as a stopgap. Recommend
-- adding a stg_client_trades.sql so marts don't read raw sources directly.
-- Source: raw.client_trades (loaded from CLIENT_TRADES.JSON)

with trades as (

    select
        trade_id,
        client_id,
        cast(trade_date as date)           as trade_date,
        instrument,
        direction,
        cast(volume_lots as decimal(18,4)) as volume_lots,
        cast(open_price as decimal(18,6))  as open_price,
        cast(close_price as decimal(18,6)) as close_price,
        cast(pnl_usd as decimal(18,2))     as pnl_usd,
        trade_status

    from {{ source('raw', 'client_trades') }}

),

dim_clients as (

    select * from {{ ref('dim_clients') }}

),

trades_with_client as (

    select
        t.*,
        c.client_key,
        c.risk_category  as client_risk_category_at_trade,
        c.account_status as client_account_status_at_trade,
        c.country,
        c.account_type

    from trades t
    left join dim_clients c
        on t.client_id = c.client_id
        and t.trade_date >= cast(c.effective_from as date)
        and t.trade_date <  cast(coalesce(c.effective_to, timestamp '9999-12-31') as date)

),

final as (

    select
        trade_id,
        client_key,
        client_id,
        trade_date,
        instrument,
        direction,
        volume_lots,
        open_price,
        close_price,
        pnl_usd,
        trade_status,
        client_risk_category_at_trade,
        client_account_status_at_trade,
        country,
        account_type,
        case when pnl_usd >= 0 then true else false end as is_winning_trade

    from trades_with_client

)

select * from final
