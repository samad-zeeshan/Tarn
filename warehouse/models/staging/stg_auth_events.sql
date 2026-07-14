-- One row per authentication event, lightly conformed.
--
-- No surrogate keys are computed here. This view sits over 1,051,430,459 rows, so anything it
-- computes is recomputed on every scan. See the header of marts/fact_auth_event.sql.

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
