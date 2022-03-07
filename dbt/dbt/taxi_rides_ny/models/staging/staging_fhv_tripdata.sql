{{config(materialized = 'view')}}

select
    {{dbt_utils.surrogate_key(['dispatching_base_num','pickup_datetime'])}} as tripid,
    dispatching_base_num,
    cast(PUlocationID as integer) as pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    SR_Flag,
    pickup_datetime,
    dropoff_datetime
from {{source('staging','fhv_tripdata')}}
where dispatching_base_num is not null
{% if var('is_test_run',default=true)%}
    limit 100
{% endif %}
