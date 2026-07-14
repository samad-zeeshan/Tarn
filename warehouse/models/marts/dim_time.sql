-- One row per day of the corpus. 58 rows.
--
-- day_index and week_index are real, they are just t / 86400. calendar_date is a label, because
-- LANL ships relative seconds and pipeline/common.py anchors t=0 arbitrarily. That is why there
-- is no day-of-week column here: it would be fiction.

{{ config(materialized='table') }}

with dates as (
    select distinct
        event_date,
        day_index
    from {{ ref('stg_auth_events') }}
),

redteam_days as (
    select distinct event_date from {{ ref('stg_redteam') }}
),

volume as (
    select
        event_date,
        count(*)                                        as auth_events,
        count(distinct src_user)                        as active_identities,
        sum(case when is_failure then 1 else 0 end)     as failures
    from {{ ref('stg_auth_events') }}
    group by event_date
)

select
    -- 58 rows. The md5 is free here and the marts join on it.
    md5(cast(dates.event_date as varchar))              as time_key,
    dates.event_date                                    as calendar_date,
    dates.day_index                                     as day_index,
    (dates.day_index / 7)::int                          as week_index,
    volume.auth_events                                  as auth_events,
    volume.active_identities                            as active_identities,
    volume.failures                                     as failures,
    (redteam_days.event_date is not null)               as has_redteam_activity
from dates
left join volume using (event_date)
left join redteam_days using (event_date)
