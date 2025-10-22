{{
  config(
    materialized='incremental',
    unique_key='conversion_session',
    partition_by={
      "field": "conversion_date",
      "data_type": "date"
    }
  )
}}

with conversions as (
    select * 
    from {{ ref('int_sessions') }}
    where is_conversion = 1
    
    {% if is_incremental() %}
        and session_date >= date_sub(current_date(), interval 2 day)
    {% endif %}
),

all_sessions as (
    select *
    from {{ ref('int_sessions') }}
    
    {% if is_incremental() %}
        where session_date >= date_sub(current_date(), interval {{ var('attribution_window_days') }} + 2 day)
    {% endif %}
),

journey as (
    select
        c.session_id as conversion_session,
        c.user_pseudo_id,
        c.session_date as conversion_date,
        c.session_start as conversion_time,
        c.revenue,
        
        s.session_id,
        s.channel,
        s.source,
        s.medium,
        s.session_start,
        
        row_number() over (
            partition by c.session_id 
            order by s.session_start asc
        ) as touch_order
        
    from conversions c
    join all_sessions s
        on c.user_pseudo_id = s.user_pseudo_id
        and s.session_start <= c.session_start
        and timestamp_diff(
            timestamp_micros(c.session_start),
            timestamp_micros(s.session_start),
            day
        ) <= {{ var('attribution_window_days') }}
)

select
    conversion_session,
    user_pseudo_id,
    conversion_date,
    conversion_time,
    revenue,
    max(case when touch_order = 1 then channel end) as attributed_channel,
    max(case when touch_order = 1 then source end) as attributed_source,
    max(case when touch_order = 1 then medium end) as attributed_medium,
    count(*) as total_touches,
    
    'first_click' as model
    
from journey
group by 1, 2, 3, 4, 5