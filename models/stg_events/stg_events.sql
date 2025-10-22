{{
  config(
    materialized='view'
  )
}}

with events as (
    select
        parse_date('%Y%m%d', event_date) as event_date,
        event_timestamp,
        event_name,
        user_pseudo_id,
        traffic_source.source as source,
        traffic_source.medium as medium,
        ecommerce.purchase_revenue as revenue,
        'ga4' as event_source

    from `{{ var('ga4_project') }}.{{ var('ga4_dataset') }}.events_20210131`
    where 
        event_name in ('session_start', 'page_view', 'purchase')
)

select 
    *,
    concat(event_name, '-', user_pseudo_id, '-', cast(event_timestamp as string)) as event_id
from events