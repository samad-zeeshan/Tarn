-- Q4: failure-ratio spikes, z-scored against each identity's own history.
--
-- A global threshold is useless here, since some service accounts fail 30% of the time forever
-- and are fine. The baseline window excludes the current day, or the spike inflates the baseline
-- it is being measured against.

select
    r.src_user                                      as identity,
    r.event_date,
    r.auth_count,
    r.failure_count,
    round(r.failure_ratio, 4)                       as failure_ratio_today,
    round(r.failure_ratio_baseline_mean, 4)         as failure_ratio_baseline,
    round(r.failure_ratio_baseline_stddev, 4)       as baseline_stddev,
    round(r.failure_ratio_zscore, 2)                as failure_zscore,
    r.baseline_days_available,
    r.distinct_dst_computers,
    r.is_redteam_day,
    i.is_machine_account
from {{ROLLUP}} r
join {{DIM_IDENTITY}} i on r.identity_key = i.identity_key
where r.failure_ratio_zscore is not null
  and r.failure_ratio_zscore > 3          -- 3 sigma against the identity's own history
  and r.failure_count >= 5                -- and not merely 1 failure out of 2 attempts
order by r.failure_ratio_zscore desc, r.failure_count desc
limit 25
