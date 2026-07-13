-- Q4 — Failure-ratio spike detection: windowed z-score, in SQL.
--
-- QUESTION: whose authentication failure rate today is anomalous *for them*?
--
-- Credential stuffing, password spraying, and a Kerberoasting attempt that guesses wrong
-- all look the same in the telemetry: a sudden burst of failures from an identity that
-- normally succeeds. A global threshold ("alert above 20% failures") is useless — some
-- service accounts fail 30% of the time forever and are fine.
--
-- So: z-score against the identity's own trailing 30-day mean and stddev, computed in the
-- mart with a window frame that EXCLUDES the current day (rows between 30 preceding and 1
-- preceding). Excluding today matters more than it sounds: if the spike is allowed into
-- its own baseline it inflates the mean and stddev and damps the very signal we want.
--
-- The z-score is NULL, not zero, when there are fewer than 3 baseline days or the baseline
-- has no variance. That is why this query can say "top 25 spikes" and mean it, rather than
-- ranking 500 identities whose entire history is one quiet day.

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
