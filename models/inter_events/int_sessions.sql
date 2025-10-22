{{
  config(
    materialized='incremental',
    unique_key='session_id',
    partition_by={
      "field": "session_date",
      "data_type": "date"
    }
  )
}}

with all_events as (
    select * from {{ ref('stg_events') }}
    union all
    select * from {{ ref('stg_streamed_events') }}
    
    {% if is_incremental() %}
        where event_date >= date_sub(current_date(), interval 2 day)
    {% endif %}
),

with_session_flag as (
    select
        *,
        case 
            when timestamp_diff(
                timestamp_micros(event_timestamp),
                lag(timestamp_micros(event_timestamp)) over (
                    partition by user_pseudo_id order by event_timestamp
                ),
                minute
            ) > {{ var('session_timeout_minutes') }}
            or lag(event_timestamp) over (
                partition by user_pseudo_id order by event_timestamp
            ) is null
            then 1 else 0
        end as new_session
    from all_events
),

with_session_id as (
    select
        *,
        concat(user_pseudo_id, '-', 
               cast(sum(new_session) over (
                   partition by user_pseudo_id 
                   order by event_timestamp
                   rows between unbounded preceding and current row
               ) as string)
        ) as session_id
    from with_session_flag
),

sessions_base as (
    select
        session_id,
        user_pseudo_id,
        date(timestamp_micros(min(event_timestamp))) as session_date,
        min(event_timestamp) as session_start,
        
        array_agg(source ignore nulls order by event_timestamp limit 1)[offset(0)] as source,
        array_agg(medium ignore nulls order by event_timestamp limit 1)[offset(0)] as medium,
        
        max(case when event_name = 'purchase' then 1 else 0 end) as is_conversion,
        sum(case when event_name = 'purchase' then revenue else 0 end) as revenue
        
    from with_session_id
    group by 1, 2
),

sessions as (
    select
        *,
        {{ get_channel() }} as channel
    from sessions_base
)

select * from sessions