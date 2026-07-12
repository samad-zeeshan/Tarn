-- Staging: the labelled red-team compromise events (LANL's ground truth).
--
-- 749 rows. This is the most valuable table in the project: it is what lets every layer
-- answer "would this have caught the attacker?" instead of "how many rows are there?".
--
-- The label identifies a compromise by the full (time, user, src_computer, dst_computer)
-- tuple, and that exact tuple also appears in auth.txt. Joining on all four is what keeps
-- the label trustworthy — joining on user alone would smear "compromised" across that
-- identity's entire benign history and quietly inflate every downstream hit rate.

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
