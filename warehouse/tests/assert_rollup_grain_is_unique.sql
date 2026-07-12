-- The grain of mart_daily_identity_rollup is (identity, day). Documenting a grain and
-- not testing it is just a comment. This fails if any identity has two rows on a day.
select
    identity_key,
    event_date,
    count(*) as n
from {{ ref('mart_daily_identity_rollup') }}
group by identity_key, event_date
having count(*) > 1
