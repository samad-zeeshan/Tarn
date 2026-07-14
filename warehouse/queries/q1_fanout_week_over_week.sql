-- Q1: lateral-movement precursor. Destination fan-out, week over week.
--
-- Week over week, not raw fan-out, because raw fan-out just ranks service accounts. A backup
-- agent legitimately hits 500 hosts a day. The signal is the change against the identity's own
-- prior week.

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
