-- The rollup comes from Spark; the fact comes from the lake. They are computed by two
-- different engines from the same source, so they had better agree.
--
-- This is the test that would have caught a bad join, a dropped partition, or a
-- silently-changed filter in either engine. It compares total auth events per day.
--
-- Tolerance is ZERO. These are counts of the same rows; "close enough" is a bug.

with from_fact as (
    select
        event_date,
        sum(event_count) as fact_events
    from {{ ref('fact_auth_event') }}
    group by event_date
),

from_rollup as (
    select
        event_date,
        sum(auth_count) as rollup_events
    from {{ ref('mart_daily_identity_rollup') }}
    group by event_date
)

select
    coalesce(from_fact.event_date, from_rollup.event_date) as event_date,
    from_fact.fact_events,
    from_rollup.rollup_events,
    from_fact.fact_events - from_rollup.rollup_events      as difference
from from_fact
full outer join from_rollup using (event_date)
where coalesce(from_fact.fact_events, -1) <> coalesce(from_rollup.rollup_events, -1)
