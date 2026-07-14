-- The 749 labelled red-team compromise events. LANL's ground truth.
--
-- This is what lets every layer answer "would this have caught the attacker" instead of "how
-- many rows are there".

{{ config(materialized='view') }}

select
    time                                        as event_time_seconds,
    "user"                                      as src_user,
    src_computer                                as src_computer,
    dst_computer                                as dst_computer,
    event_date                                  as event_date,
    day_index                                   as day_index,
    hour_of_day                                 as hour_of_day,

    -- 749 rows, so an md5 here costs nothing and the marts join on it.
    md5("user")                                 as identity_key

from {{ source('lake', 'redteam') }}
