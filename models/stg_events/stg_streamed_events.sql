{{
  config(
    materialized='incremental',
    unique_key='event_id',
    partition_by={
      "field": "event_date",
      "data_type": "date"
    }
  )
}}

with events as (
    select
        parse_date('%Y%m%d', event_date) as event_date,
        event_timestamp,
        event_name,
        user_pseudo_id,
        traffic_source as source,
        traffic_medium as medium,
        purchase_revenue as revenue,
        'streaming' as event_source
        
    from {{ source('streaming', 'streaming_events') }}
    where event_name in ('session_start', 'page_view', 'purchase')
    
    {% if is_incremental() %}
        and event_timestamp > (select coalesce(max(event_timestamp), 0) from {{ this }})
    {% endif %}
)

select 
    *,
    concat(event_name, '-', user_pseudo_id, '-', cast(event_timestamp as string)) as event_id
from events