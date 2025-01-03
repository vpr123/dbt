{{ config(materialized='table') }}

SELECT r.kafka_partition,
       MIN(t.first_missing_offset) AS first_missing_offset,
       r.last_offset,
       NVL(SUM(t.last_missing_offset - t.first_missing_offset + 1),0) AS missing_offsets_cnt
  FROM (SELECT kafka_partition,
               MAX(last_offset) AS last_offset
          FROM {{ ref('event_continuous_ranges') }}
         GROUP BY kafka_partition) r
  LEFT OUTER JOIN {{ ref('kafka_missing_offsets') }} t
    ON t.kafka_partition = r.kafka_partition
 GROUP BY r.kafka_partition,
          r.last_offset