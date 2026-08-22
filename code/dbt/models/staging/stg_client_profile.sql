-- Staging: baseline profile snapshot (one row per client, "as last known" at
-- extract time). This is the pre-CDC baseline used as version 1 of each
-- client's history -- point-in-time versioning is built downstream in
-- scripts/cdc_processor.py + marts/dim_clients.sql, not here.
-- Source: raw.client_profile (loaded from CLIENT_PROFILE.JSON)

with source as (

    select * from {{ source('raw', 'client_profile') }}

),

renamed as (

    select
        client_id,
        full_name,
        cast(date_of_birth as date)                as date_of_birth,
        nationality,
        risk_category,
        cast(account_balance_usd as decimal(18,2)) as account_balance_usd,
        account_status,
        currency,
        cast(last_login_date as date)              as last_login_date,
        preferred_language,

        -- data quality: implausible date_of_birth (e.g. CL025 = 1888-12-19).
        -- flagged, not corrected -- fixing a DOB requires a source-side
        -- correction, not a staging-layer guess.
        case
            when date_of_birth < date '1900-01-01' then true
            else false
        end                                          as is_dob_suspect

    from source

)

select * from renamed
