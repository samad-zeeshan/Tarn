-- Q2 — Off-hours authentication share, each identity against its OWN baseline.
--
-- QUESTION: who is suddenly working at hours they never worked before?
--
-- READ THE CAVEAT. LANL has no wall-clock time — only seconds since collection started.
-- "3am" is not a fact in this corpus. What IS a fact is the diurnal cycle: pipeline/
-- diurnal.py measures the volume-by-hour curve, finds the trough, and that measured band
-- is what `is_off_hours` means here. So this query does not ask "who worked at 3am"; it
-- asks "who worked during the hours when this network is measurably asleep", which is the
-- question that actually survives a screening interview.
--
-- Scoring each identity against its own baseline (rather than a global threshold) is what
-- separates "the night-shift admin, as always" from "the day-shift user who has never once
-- logged in during the trough and just did it forty times".

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
