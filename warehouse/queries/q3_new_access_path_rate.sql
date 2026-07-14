-- Q3: new access paths. First-time (identity -> computer) edges per day.
--
-- The edge set is the privilege graph, so this measures it growing. Every new edge is a path
-- that did not exist yesterday.

with network_daily as (
    select
        'NETWORK'                               as scope,
        cast(r.event_date as varchar)           as event_date,
        null                                    as identity,
        sum(r.new_dst_computers)                as new_edges,
        sum(r.distinct_dst_computers)           as total_edges_touched,
        count(distinct r.identity_key)          as active_identities,
        sum(r.is_redteam_day::int)              as redteam_identities_active
    from {{ROLLUP}} r
    group by 1, 2, 3
),

identity_peaks as (
    select
        'IDENTITY'                              as scope,
        cast(r.event_date as varchar)           as event_date,
        r.src_user                              as identity,
        r.new_dst_computers                     as new_edges,
        r.distinct_dst_computers                as total_edges_touched,
        1                                       as active_identities,
        r.is_redteam_day::int                   as redteam_identities_active
    from {{ROLLUP}} r
    join {{DIM_IDENTITY}} i on r.identity_key = i.identity_key
    where not i.is_machine_account
      and r.new_dst_computers > 0
    order by r.new_dst_computers desc
    limit 20
)

select * from network_daily
union all
select * from identity_peaks
order by scope, new_edges desc, event_date
