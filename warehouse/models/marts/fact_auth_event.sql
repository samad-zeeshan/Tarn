-- fact_auth_event — the grain of the whole warehouse.
--
-- GRAIN: ONE ROW PER AUTHENTICATION EVENT. Not per session, not per identity-day. If a
--        user authenticates to the same host twice in the same second (which happens —
--        LANL's clock has 1-second resolution), that is two rows. Nothing here
--        deduplicates, because deduplicating would silently destroy the failure counts.
--
--
-- WHY THIS IS A VIEW AND NOT A TABLE — the central lakehouse decision in the project.
-- ---------------------------------------------------------------------------------
-- The first build materialized this as a table. It was killed at 35 GB and still climbing,
-- to duplicate a 9.9 GB Parquet lake sitting on the same disk. So the fact stays in the
-- lake and the warehouse serves it as a view: DuckDB reads the Parquet in place with
-- projection and predicate pushdown, and the date partitioning prunes files for any query
-- with a date filter. The dimensions and the aggregate marts — small, and read constantly —
-- ARE materialized as tables. That is the lakehouse pattern, not a concession: one copy of
-- the atomic data in an open columnar format, with the warehouse supplying the schema, the
-- keys, and the tests over it.
--
--
-- WHY THERE ARE NO SURROGATE KEYS ON THIS FACT — a decision that got reversed.
-- ---------------------------------------------------------------------------
-- This model originally carried four md5 surrogate keys (identity_key, src_computer_key,
-- dst_computer_key, time_key), because that is what you are supposed to do in a star schema.
-- On this corpus it was wrong twice over:
--
--   1. As a TABLE, an md5 is a unique 32-character string, so it does not compress:
--      4 keys x 32 bytes x 1.05e9 rows is ~134 GB of key data alone.
--   2. As a VIEW, the md5s are recomputed on every scan. Ten dbt tests over the fact meant
--      ~40 billion md5 computations to test data that had not changed.
--
-- The point of a surrogate key is to insulate the warehouse from natural keys that are wide,
-- mutable, or meaningless. LANL's natural keys are none of those: `U292@DOM1` and `C1065`
-- are short, immutable within a 58-day snapshot, and dictionary-encoded by Parquet into a
-- couple of bytes per row. Hashing them bought nothing and cost enormously — so the fact
-- joins to the dimensions on the natural keys, and the `relationships` tests in _marts.yml
-- enforce referential integrity across all 1.05 billion rows either way.
--
-- The dimensions and the small aggregate marts DO keep an md5 `identity_key` — there the
-- cost is trivial (80k rows) and a stable hash is genuinely useful as a join key.
--
--
-- Degenerate dimensions (auth_type, logon_type, auth_orientation, outcome) live on the fact
-- rather than in a junk dimension: low cardinality, always queried alongside the measure,
-- and a junk dim would buy nothing but a join.
--
-- is_off_hours comes from the MEASURED diurnal band (pipeline/diurnal.py -> bench/diurnal.json),
-- passed in as a dbt var. It is never a hard-coded 9-to-5 — see dim_time's header for why
-- that would be fiction on this corpus.

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
    -- Foreign keys — the NATURAL keys. See the header.
    events.src_user                                 as src_user,        -- FK -> dim_identity.identity_name
    events.src_computer                             as src_computer,    -- FK -> dim_computer.computer_name
    events.dst_computer                             as dst_computer,    -- FK -> dim_computer.computer_name
    events.event_date                               as event_date,      -- FK -> dim_time.calendar_date

    -- Event attributes / degenerate dimensions
    events.event_time_seconds                       as event_time_seconds,
    events.event_ts                                 as event_ts,
    events.hour_of_day                              as hour_of_day,
    events.dst_user                                 as dst_user,
    events.auth_type                                as auth_type,
    events.logon_type                               as logon_type,
    events.auth_orientation                         as auth_orientation,
    events.outcome                                  as outcome,
    events.src_is_machine                           as src_is_machine,

    -- Measures (additive)
    1                                               as event_count,
    case when events.is_success then 1 else 0 end   as success_count,
    case when events.is_failure then 1 else 0 end   as failure_count,

    -- Flags
    {% set band = var('off_hours_band', []) %}
    {% if band %}
    (events.hour_of_day in ({{ band | join(', ') }}))
    {% else %}
    false
    {% endif %}                                     as is_off_hours,

    -- Is this exact event one of the 749 labelled compromises? Joined on the FULL tuple:
    -- joining on user alone would smear "compromised" across that identity's entire benign
    -- history and quietly inflate every recall number in Q5.
    (redteam.src_user is not null)                  as is_redteam

from events
left join redteam
    on  events.event_time_seconds = redteam.event_time_seconds
    and events.src_user           = redteam.src_user
    and events.src_computer       = redteam.src_computer
    and events.dst_computer       = redteam.dst_computer
