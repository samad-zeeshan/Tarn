-- dim_identity — one row per authenticating identity (human user OR machine account).
--
-- GRAIN: one row per src_user string as it appears in the corpus (e.g. 'U292@DOM1',
--        'C1065$@DOM1', 'ANONYMOUS LOGON@C586').
-- SCD:   Type 1. The corpus is a 58-day snapshot with no identity mutation events in it —
--        a user never gets renamed mid-stream — so there is no history to slowly change.
--        Attributes here are corpus-wide aggregates, and rebuilding overwrites them.
--        Stating this explicitly because "we chose Type 1" and "we didn't think about
--        SCDs" look identical in a schema.
--
-- Machine accounts (trailing '$') are ~the majority of traffic. They are kept, not
-- filtered: whether to exclude them is an analytical decision each query makes for
-- itself, and hiding them here would silently change every downstream number.

{{ config(materialized='table') }}

with events as (
    select * from {{ ref('stg_auth_events') }}
),

compromised as (
    select distinct src_user as identity_name from {{ ref('stg_redteam') }}
),

agg as (
    select
        src_user                                        as identity_name,
        any_value(src_user_name)                        as user_name,
        any_value(src_domain)                           as domain,
        any_value(src_is_machine)                       as is_machine_account,

        count(*)                                        as total_auth_events,
        sum(case when is_success then 1 else 0 end)     as total_success,
        sum(case when is_failure then 1 else 0 end)     as total_failure,
        count(distinct dst_computer)                    as lifetime_distinct_destinations,
        count(distinct src_computer)                    as lifetime_distinct_sources,
        count(distinct event_date)                      as active_days,
        min(event_date)                                 as first_seen_date,
        max(event_date)                                 as last_seen_date
    from events
    group by src_user
)

select
    -- The dimension DOES keep an md5 key: 80k rows, so the cost is nothing, and the small
    -- aggregate marts join on it. It is the 1.05e9-row fact that could not afford one.
    md5(agg.identity_name)                              as identity_key,
    agg.*,
    case when agg.total_auth_events > 0
         then agg.total_failure * 1.0 / agg.total_auth_events
         else 0 end                                     as lifetime_failure_ratio,
    (compromised.identity_name is not null)             as is_compromised
from agg
left join compromised using (identity_name)
