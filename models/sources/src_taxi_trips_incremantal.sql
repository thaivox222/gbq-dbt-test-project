{{ config(
	materialized='incremental'
	,alias= 'src_taxi_incremental'
    ,incremental_strategy = 'merge') }}


select *
from {{ source('s_taxi_schema', 's_taxi_table') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
 where trip_start_timestamp > (select max(trip_start_timestamp) from {{ this }})
 and tips > 0
 
{% endif %}

