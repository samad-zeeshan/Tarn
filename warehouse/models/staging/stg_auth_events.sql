-- Staging: one row per authentication event, lightly conformed.
--
-- NO SURROGATE KEYS ARE COMPUTED HERE, deliberately. This view sits over 1,051,430,459 rows
-- and is a VIEW, so anything it computes is recomputed on every scan. An earlier version
-- emitted four md5 surrogate keys; across the dbt test suite that worked out to ~40 billion
-- md5 computations, to hash natural keys (`U292@DOM1`, `C1065`) that are already short,
-- immutable, and dictionary-encoded by Parquet.
--
-- See the header of marts/fact_auth_event.sql for the full reasoning. The dimensions and the
-- small aggregate marts still carry an md5 `identity_key` — at 80k rows the cost is nothing
-- and a stable hash is a genuinely convenient join key there.

{{ config(materialized='view') }}

select
    -- Degenerate: LANL's raw relative-second clock. Kept because it is the only true
    -- ordering key in the corpus, and the graph + streaming stages both join on it.
    time                                        as event_time_seconds,
    event_ts                                    as event_ts,
    event_date                                  as event_date,
    day_index                                   as day_index,
    hour_of_day                                 as hour_of_day,

    src_user                                    as src_user,
    dst_user                                    as dst_user,
    src_computer                                as src_computer,
    dst_computer                                as dst_computer,

    auth_type                                   as auth_type,
    logon_type                                  as logon_type,
    auth_orientation                            as auth_orientation,
    outcome                                     as outcome,
    is_success                                  as is_success,
    is_failure                                  as is_failure,

    src_user_name                               as src_user_name,
    src_domain                                  as src_domain,
    src_is_machine                              as src_is_machine

from {{ source('lake', 'auth') }}
where src_user is not null
