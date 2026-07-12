-- mart_daily_identity_rollup — the analytics-facing aggregate. One row per identity-day.
--
-- GRAIN: one row per (identity_key, event_date).
--
-- This is a *pass-through* of what Spark already computed in Stage 1
-- (pipeline/rollup.py), not a recomputation of it. That is deliberate: the expensive
-- distributed aggregation belongs in Spark, and having the warehouse recompute the same
-- numbers in DuckDB would be both slower and a second source of truth for figures the
-- site quotes. dbt's job here is to conform it into the star (attach the surrogate keys),
-- add the peer-relative measures that are natural in SQL, and test it.
--
-- The baseline columns are what turn a raw count into a signal: 30 destinations is
-- unremarkable for a service account and alarming for a workstation user, so every
-- behavioural measure is also expressed relative to that identity's OWN trailing history.

{{ config(materialized='table') }}

with rollup as (
    select
        md5(src_user)                       as identity_key,
        src_user,
        event_date,
        auth_count,
        success_count,
        failure_count,
        failure_ratio,
        distinct_dst_computers,
        distinct_src_computers,
        new_dst_computers,
        off_hours_events,
        off_hours_share,
        is_redteam_day
    from {{ source('lake', 'rollup') }}
),

with_baselines as (
    select
        *,
        md5(cast(event_date as varchar))    as time_key,

        -- Each identity's own trailing baseline, EXCLUDING today (rows between 30 preceding
        -- and 1 preceding). Including today would let a spike inflate its own baseline and
        -- damp the very signal we are trying to detect.
        avg(distinct_dst_computers) over (
            partition by identity_key order by event_date
            rows between 30 preceding and 1 preceding
        )                                   as fanout_baseline_mean,
        stddev_samp(distinct_dst_computers) over (
            partition by identity_key order by event_date
            rows between 30 preceding and 1 preceding
        )                                   as fanout_baseline_stddev,
        avg(failure_ratio) over (
            partition by identity_key order by event_date
            rows between 30 preceding and 1 preceding
        )                                   as failure_ratio_baseline_mean,
        stddev_samp(failure_ratio) over (
            partition by identity_key order by event_date
            rows between 30 preceding and 1 preceding
        )                                   as failure_ratio_baseline_stddev,
        avg(off_hours_share) over (
            partition by identity_key order by event_date
            rows between 30 preceding and 1 preceding
        )                                   as off_hours_share_baseline_mean,

        count(*) over (
            partition by identity_key order by event_date
            rows between 30 preceding and 1 preceding
        )                                   as baseline_days_available
    from rollup
)

select
    *,
    -- Z-scores against the identity's own trailing baseline. NULL (not 0, not Infinity)
    -- when the baseline is too thin or has no variance — a z-score computed from 1 prior
    -- day is noise wearing a lab coat, and emitting it as a number would let it rank.
    case
        when baseline_days_available >= 3 and fanout_baseline_stddev > 0
        then (distinct_dst_computers - fanout_baseline_mean) / fanout_baseline_stddev
    end                                     as fanout_zscore,

    case
        when baseline_days_available >= 3 and failure_ratio_baseline_stddev > 0
        then (failure_ratio - failure_ratio_baseline_mean) / failure_ratio_baseline_stddev
    end                                     as failure_ratio_zscore

from with_baselines
