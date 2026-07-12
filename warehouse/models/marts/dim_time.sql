-- dim_time — one row per day of the corpus.
--
-- GRAIN: one row per event_date (day). 58 rows.
-- SCD:   n/a — a date dimension is immutable by construction.
--
-- READ THIS BEFORE USING THE CALENDAR COLUMNS. LANL publishes `time` as seconds elapsed
-- since the start of collection. There is no wall-clock timestamp anywhere in the corpus.
-- pipeline/common.py anchors t=0 to 2015-01-01 purely so the lake has a partition key and
-- this dimension has a date. That anchor is ARBITRARY:
--
--   * day_index and week_index are REAL (they are just t / 86400).
--   * calendar_date is a LABEL, not an observation.
--   * day-of-week would be fiction, so this dimension does not expose one, and no model
--     in the project uses one.
--
-- Hour-of-day is a different story: (t mod 86400) is a real offset within the collection
-- day, and pipeline/diurnal.py MEASURES the volume curve to show the diurnal cycle is
-- genuinely there. The off-hours band lives on the fact, derived from that measurement.

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
