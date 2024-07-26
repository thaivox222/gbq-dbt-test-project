{{ config(materialized="table")
 }}

WITH 
cte_1 AS(
SELECT *
FROM {{ ref('int_top3_taxi') }}
)
, cte_2 AS(
-- считаем чаевые по всем такси помесячно с апреля 2018
SELECT taxi_id, FORMAT_DATE('%Y-%m',trip_start_timestamp) as year_month
,sum(tips) as tips_sum
FROM {{ ref('src_taxi_trips_select') }}
GROUP BY taxi_id, year_month
ORDER BY  taxi_id, year_month ASC
)
-- сделаем джойн cte_1+cte_2
, cte_3 AS(
SELECT cte_2.*
FROM cte_1
INNER JOIN cte_2 ON cte_1.taxi_id = cte_2.taxi_id
)
-- считаем разницу с предыыдущим месяцем по такси из ТОП-3
SELECT *
,ROUND((tips_sum - LAG(tips_sum) OVER(PARTITION BY taxi_id ORDER BY taxi_id, year_month))/tips_sum*100,2) as tips_change
FROM cte_3
ORDER BY taxi_id, year_month