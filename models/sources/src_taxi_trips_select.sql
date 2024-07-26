with cte_0 as 
(
select taxi_id, trip_start_timestamp, tips
from {{ source('s_taxi_schema', 's_taxi_table') }}
where trip_start_timestamp between '2018-04-01 00:00:00' and  CURRENT_TIMESTAMP()
and tips > 0
)
select * from cte_0
