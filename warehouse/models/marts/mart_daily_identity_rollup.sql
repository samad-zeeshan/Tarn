-- One row per identity per day. The aggregate that Q1 to Q5 and the demo actually read.
--
-- A pass-through of what Spark already computed, conformed into the star and enriched with each
-- identity's own trailing baseline.

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
    -- when the baseline is too thin or has no variance, a z-score computed from 1 prior
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
