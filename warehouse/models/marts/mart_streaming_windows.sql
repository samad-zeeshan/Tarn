-- The Stage 3 streaming sink, conformed into the same star. One row per identity per window.
--
-- The point is that there is no second warehouse. Structured Streaming writes Parquet and dbt
-- reads it with the same keys and the same dimensions as the batch fact.

{{ config(
    materialized='table',
    enabled=var('streaming_enabled', true)
) }}

with windows as (
    select
        window_start,
        window_end,
        src_user,
        auth_count,
        failure_count,
        success_count,
        distinct_dst_computers,
        distinct_src_computers,
        max_produce_ts_ms,
        event_date
    from {{ source('lake', 'streaming_windows') }}
)

select
    md5(concat_ws('|', src_user, cast(window_start as varchar)))    as window_key,
    md5(src_user)                                                   as identity_key,
    md5(cast(event_date as varchar))                                as time_key,

    src_user,
    window_start,
    window_end,
    event_date,

    auth_count,
    success_count,
    failure_count,
    case when auth_count > 0
         then failure_count * 1.0 / auth_count
         else 0 end                                                 as failure_ratio,

    -- Approximate, HyperLogLog, not an exact distinct. See the model header.
    distinct_dst_computers      as distinct_dst_computers_approx,
    distinct_src_computers      as distinct_src_computers_approx

from windows
