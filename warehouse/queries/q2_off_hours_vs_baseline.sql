-- Q2: off-hours authentication, each identity against its own baseline.
--
-- "3am" is not a fact in this corpus. The band comes from the measured diurnal trough in
-- bench/diurnal.json, so this asks who worked while the network is measurably asleep, not who
-- worked at a clock hour we guessed.

with daily as (
    select
        r.identity_key,
        r.src_user,
        r.event_date,
        r.auth_count,
        r.off_hours_events,
        r.off_hours_share,
        r.off_hours_share_baseline_mean,
        r.baseline_days_available,
        r.is_redteam_day
    from {{ROLLUP}} r
    join {{DIM_IDENTITY}} i on r.identity_key = i.identity_key
    where not i.is_machine_account
      -- An identity with a handful of events a day produces a meaningless share (1 of 2
      -- events off-hours = 50%). Require enough volume for the ratio to mean something.
      and r.auth_count >= 10
)

select
    src_user                                        as identity,
    event_date,
    auth_count,
    off_hours_events,
    round(off_hours_share, 4)                       as off_hours_share_today,
    round(off_hours_share_baseline_mean, 4)         as off_hours_share_baseline,
    round(
        off_hours_share - off_hours_share_baseline_mean, 4
    )                                               as excess_over_own_baseline,
    baseline_days_available,
    is_redteam_day
from daily
where baseline_days_available >= 3
  and off_hours_share > 0
  -- The identity's own history says it essentially never works the trough...
  and off_hours_share_baseline_mean < 0.05
  -- ...and today it did, materially.
  and off_hours_share > 0.25
order by excess_over_own_baseline desc, off_hours_events desc
limit 25
