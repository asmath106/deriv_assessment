-- Staging: one row per client signup event, light typing/cleanup only.
-- Source: raw.client_signup (loaded from CLIENT_SIGNUP.JSON)

with source as (

    select * from {{ source('raw', 'client_signup') }}

),

renamed as (

    select
        client_id,
        cast(signup_date as date)      as signup_date,
        trim(country)                  as country,
        trim(lower(email))             as email,
        referral_source,
        account_type,
        kyc_status,
        signup_platform,
        nullif(trim(promo_code), '')   as promo_code,
        assigned_manager

    from source

)

select * from renamed
