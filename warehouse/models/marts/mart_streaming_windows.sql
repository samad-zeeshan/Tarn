-- mart_streaming_windows — the Stage-3 streaming sink, conformed into the same star.
--
-- GRAIN: one row per (identity, 1-minute event-time window).
--
-- The point of this model is that there is no second warehouse. The Structured Streaming
-- job writes Parquet; dbt reads it with the same conformance rules, the same surrogate
-- keys, and the same dimension joins as the batch fact. A screener can join a streaming
-- window to dim_identity and it just works — which is the actual argument for a lakehouse
-- over a bolted-on "real-time database".
--
-- Absent until Stage 3 has run. Guarded so `dbt build` still succeeds on a fresh clone that
-- has only done Stages 1-2 (CI does exactly that).
--
-- distinct_dst_computers here is an APPROXIMATE count (approx_count_distinct / HyperLogLog).
-- That is a deliberate streaming-vs-batch tradeoff: exact distinct counts require holding
-- every seen value in state per window, and the batch layer already gives exact numbers.
-- The column name would be a lie if it did not say so — see the description in _marts.yml.

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

    -- Approximate — HyperLogLog, not an exact distinct. See the model header.
    distinct_dst_computers      as distinct_dst_computers_approx,
    distinct_src_computers      as distinct_src_computers_approx

from windows
