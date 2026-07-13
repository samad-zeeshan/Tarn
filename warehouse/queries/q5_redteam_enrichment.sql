-- Q5 — Red-team enrichment: would Q1-Q4 actually have surfaced the attacker?
--
-- This is the query the whole warehouse exists to answer, and the only honest way to
-- answer it is as a DETECTION EVALUATION, not a highlight reel. So each of the four
-- signals is treated as a detector, run over every identity-day in the corpus, and scored
-- against LANL's ground truth:
--
--   recall     of the identity-days that really were compromised, what fraction did this
--              detector flag?  (the misses are the part everyone leaves out)
--   precision  of the identity-days this detector flagged, what fraction really were
--              compromised?
--   alerts     how many identity-days a human would have to triage. A detector with
--              perfect recall and 40,000 alerts a day is not a detector, it is a denial of
--              service against your SOC.
--   lift       precision / base_rate. How much better than picking an identity-day at
--              random. This is the number that says whether the signal is real at all.
--
-- Expect unflattering numbers. Precision on a corpus where compromised identity-days are
-- ~0.001% of all identity-days is going to be brutal for every single-signal detector, and
-- saying so is the point. A portfolio project that reports 100% recall and hides the alert
-- volume is lying by omission.

with scored as (
    select
        r.identity_key,
        r.event_date,
        r.is_redteam_day,

        -- The four detectors, expressed as the same booleans Q1-Q4 rank on.
        (r.fanout_zscore is not null and r.fanout_zscore > 3)                as d1_fanout_spike,
        (r.baseline_days_available >= 3
            and r.off_hours_share_baseline_mean < 0.05
            and r.off_hours_share > 0.25
            and r.auth_count >= 10)                                          as d2_off_hours,
        (r.new_dst_computers >= 5)                                           as d3_new_paths,
        (r.failure_ratio_zscore is not null
            and r.failure_ratio_zscore > 3
            and r.failure_count >= 5)                                        as d4_failure_spike
    from {{ROLLUP}} r
),

flagged as (
    select
        *,
        (d1_fanout_spike or d2_off_hours or d3_new_paths or d4_failure_spike) as d_any,
        (d1_fanout_spike::int + d2_off_hours::int
            + d3_new_paths::int + d4_failure_spike::int) >= 2                 as d_two_or_more
    from scored
),

totals as (
    select
        count(*)                                    as all_identity_days,
        sum(is_redteam_day::int)                    as redteam_identity_days
    from flagged
),

-- One row per detector. UNPIVOT by hand: five UNION ALL branches, each computing the same
-- four numbers for a different flag column.
per_detector as (
    select 'Q1 fan-out spike (z>3 vs own baseline)' as detector,
           sum(d1_fanout_spike::int)                                     as alerts,
           sum((d1_fanout_spike and is_redteam_day)::int)                as true_positives
    from flagged
    union all
    select 'Q2 off-hours vs own baseline',
           sum(d2_off_hours::int),
           sum((d2_off_hours and is_redteam_day)::int)
    from flagged
    union all
    select 'Q3 new access paths (>=5 new hosts/day)',
           sum(d3_new_paths::int),
           sum((d3_new_paths and is_redteam_day)::int)
    from flagged
    union all
    select 'Q4 failure-ratio spike (z>3)',
           sum(d4_failure_spike::int),
           sum((d4_failure_spike and is_redteam_day)::int)
    from flagged
    union all
    select 'ANY of Q1-Q4',
           sum(d_any::int),
           sum((d_any and is_redteam_day)::int)
    from flagged
    union all
    select 'TWO OR MORE of Q1-Q4',
           sum(d_two_or_more::int),
           sum((d_two_or_more and is_redteam_day)::int)
    from flagged
)

select
    p.detector,
    t.redteam_identity_days                                          as redteam_days_total,
    p.true_positives                                                 as redteam_days_caught,
    t.redteam_identity_days - p.true_positives                       as redteam_days_MISSED,
    round(100.0 * p.true_positives / nullif(t.redteam_identity_days, 0), 1)
                                                                     as recall_pct,
    p.alerts                                                         as alerts_raised,
    round(100.0 * p.true_positives / nullif(p.alerts, 0), 3)         as precision_pct,
    t.all_identity_days                                              as identity_days_scanned,
    round(
        (p.true_positives * 1.0 / nullif(p.alerts, 0))
        / nullif(t.redteam_identity_days * 1.0 / t.all_identity_days, 0), 1
    )                                                                as lift_over_random
from per_detector p
cross join totals t
order by recall_pct desc nulls last, precision_pct desc nulls last
