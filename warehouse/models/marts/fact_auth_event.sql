-- The atomic fact. One row per authentication event, all 1,051,430,459 of them.
--
-- Nothing here deduplicates. LANL's clock has 1-second resolution, so the same user hitting the
-- same host twice in a second is genuinely two rows, and collapsing them would destroy the
-- failure counts.

-- A view, not a table. Materializing this was killed at 35 GB and still climbing, to duplicate a
-- 9.9 GB Parquet lake sitting on the same disk. DuckDB reads the Parquet in place with
-- projection and predicate pushdown, and the date partitioning prunes files. The dimensions and
-- the aggregate marts, which are small and read constantly, are the things that get materialized.
{{ config(materialized=var('fact_materialization', 'view')) }}

with events as (
    select * from {{ ref('stg_auth_events') }}
),

redteam as (
    select distinct
        event_time_seconds,
        src_user,
        src_computer,
        dst_computer
    from {{ ref('stg_redteam') }}
)

select
    -- Natural keys, not surrogates. This model used to carry four md5 keys, which on a view get
    -- recomputed on every scan: about 40 billion md5 calls across the test suite, to hash keys
    -- like 'U292@DOM1' and 'C1065' that are already short, immutable, and dictionary encoded by
    -- Parquet. The relationships tests still cover every row.
    events.src_user                                 as src_user,
    events.src_computer                             as src_computer,
    events.dst_computer                             as dst_computer,
    events.event_date                               as event_date,

    events.event_time_seconds                       as event_time_seconds,
    events.event_ts                                 as event_ts,
    events.hour_of_day                              as hour_of_day,
    events.dst_user                                 as dst_user,
    events.auth_type                                as auth_type,
    events.logon_type                               as logon_type,
    events.auth_orientation                         as auth_orientation,
    events.outcome                                  as outcome,
    events.src_is_machine                           as src_is_machine,

    1                                               as event_count,
    case when events.is_success then 1 else 0 end   as success_count,
    case when events.is_failure then 1 else 0 end   as failure_count,

    -- From the measured band in bench/diurnal.json, never a hard-coded nine-to-five.
    {% set band = var('off_hours_band', []) %}
    {% if band %}
    (events.hour_of_day in ({{ band | join(', ') }}))
    {% else %}
    false
    {% endif %}                                     as is_off_hours,

    (redteam.src_user is not null)                  as is_redteam

from events
-- Joined on the full tuple. Joining on user alone would smear "compromised" across that
-- identity's entire benign history and inflate every recall number in Q5.
left join redteam
    on  events.event_time_seconds = redteam.event_time_seconds
    and events.src_user           = redteam.src_user
    and events.src_computer       = redteam.src_computer
    and events.dst_computer       = redteam.dst_computer
