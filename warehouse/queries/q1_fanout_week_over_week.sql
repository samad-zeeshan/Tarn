-- Q1 — Lateral-movement precursor: destination fan-out, week over week.
--
-- QUESTION: which identities are reaching many more distinct hosts this week than they did
--           last week? An account that touched 3 machines a week for a month and suddenly
--           touches 40 is the classic precursor to lateral movement.
--
-- WHY WEEK-OVER-WEEK AND NOT RAW FAN-OUT: raw fan-out just ranks service accounts. A
-- backup agent legitimately hits 500 hosts every single day and is the least interesting
-- row in the corpus. The signal is the CHANGE against the identity's own prior week, so
-- every identity is scored against itself.
--
-- Machine accounts are excluded here (not in the model — see dim_identity) because this
-- specific question is about human-shaped lateral movement.

with weekly as (
    select
        r.identity_key,
        r.src_user,
        t.week_index,
        sum(r.auth_count)                       as auth_events,
        max(r.distinct_dst_computers)           as peak_daily_fanout,
        sum(r.new_dst_computers)                as new_destinations,
        max(r.is_redteam_day::int)::boolean     as had_redteam_activity
    from {{ROLLUP}} r
    join {{DIM_TIME}} t on r.time_key = t.time_key
    join {{DIM_IDENTITY}} i on r.identity_key = i.identity_key
    where not i.is_machine_account
    group by 1, 2, 3
),

week_over_week as (
    select
        *,
        lag(peak_daily_fanout) over (
            partition by identity_key order by week_index
        )                                       as prev_week_peak_fanout
    from weekly
)

select
    src_user                                    as identity,
    week_index,
    prev_week_peak_fanout,
    peak_daily_fanout,
    peak_daily_fanout - prev_week_peak_fanout   as fanout_delta,
    round(
        peak_daily_fanout * 1.0 / nullif(prev_week_peak_fanout, 0), 2
    )                                           as fanout_multiple,
    new_destinations,
    auth_events,
    had_redteam_activity
from week_over_week
where prev_week_peak_fanout is not null
  and prev_week_peak_fanout > 0
  and peak_daily_fanout > prev_week_peak_fanout
order by fanout_delta desc, fanout_multiple desc
limit 25
