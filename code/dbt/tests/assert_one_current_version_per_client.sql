-- Singular test: exactly one is_current = true row per client_id in
-- dim_clients. A non-empty result means the SCD2 replay has a bug -- e.g. a
-- version wasn't closed out (effective_to/is_current) before the next one
-- opened. Passes when this returns zero rows.

select
    client_id,
    count(*) as current_version_count
from {{ ref('dim_clients') }}
where is_current = true
group by client_id
having count(*) > 1
