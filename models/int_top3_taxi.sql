{{ config(materialized="table")
 }}

WITH 
cte_1 AS(
-- считаем ТОП-3 taxi_id по-чаевым в апреле 2018
SELECT taxi_id, sum(tips) as tips_sum
FROM {{ ref('src_taxi_trips_select') }}
WHERE trip_start_timestamp BETWEEN '2018-04-01 00:00:00' AND '2018-04-30 00:00:00'
GROUP BY taxi_id
ORDER BY tips_sum DESC
LIMIT 3
)
SELECT * FROM cte_1 