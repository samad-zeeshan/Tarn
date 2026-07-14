-- One row per host, whether it appeared as a source, a destination, or both.
--
-- The union matters. Server-like hosts appear almost only as destinations, and without them the
-- fact's dst_computer key would fail its relationships test.

{{ config(materialized='table') }}

with sources as (
    select
        src_computer                                as computer_name,
        count(*)                                    as events_as_source,
        0                                           as events_as_destination,
        count(distinct src_user)                    as distinct_identities_from,
        0                                           as distinct_identities_to
    from {{ ref('stg_auth_events') }}
    where src_computer is not null
    group by 1
),

destinations as (
    select
        dst_computer                                as computer_name,
        0                                           as events_as_source,
        count(*)                                    as events_as_destination,
        0                                           as distinct_identities_from,
        count(distinct src_user)                    as distinct_identities_to
    from {{ ref('stg_auth_events') }}
    where dst_computer is not null
    group by 1
),

combined as (
    select * from sources
    union all
    select * from destinations
),

rolled as (
    select
        computer_name,
        sum(events_as_source)                       as events_as_source,
        sum(events_as_destination)                  as events_as_destination,
        max(distinct_identities_from)               as distinct_identities_from,
        max(distinct_identities_to)                 as distinct_identities_to
    from combined
    group by computer_name
),

pivot_hosts as (
    -- The four hosts the red team launched from. Tiny table, huge analytical value:
    -- it is the entry point for the Stage-4 blast-radius queries.
    select distinct src_computer as computer_name from {{ ref('stg_redteam') }}
),

targeted as (
    select distinct dst_computer as computer_name from {{ ref('stg_redteam') }}
)

select
    md5(rolled.computer_name)                                   as computer_key,
    rolled.*,
    (rolled.events_as_source + rolled.events_as_destination)    as total_events,
    -- A crude but useful server/workstation split: hosts that are overwhelmingly
    -- authenticated *to* and rarely *from* behave like servers.
    case
        when rolled.events_as_source = 0 then 'destination_only'
        when rolled.events_as_destination = 0 then 'source_only'
        when rolled.events_as_destination > rolled.events_as_source * 10 then 'server_like'
        else 'workstation_like'
    end                                                         as host_role,
    (pivot_hosts.computer_name is not null)                     as is_redteam_pivot,
    (targeted.computer_name is not null)                        as is_redteam_target
from rolled
left join pivot_hosts using (computer_name)
left join targeted using (computer_name)
