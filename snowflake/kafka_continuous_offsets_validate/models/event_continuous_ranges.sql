{{ config(materialized='table') }}

WITH events AS
(
 SELECT DISTINCT
        t1.kafka_partition,
        t1.kafka_offset
   FROM kafka_events t1
   LEFT OUTER JOIN kafka_offsets_stat t2
     ON t1.kafka_partition = t2.kafka_partition
  WHERE t2.kafka_partition IS NULL 
     OR t1.kafka_offset >= COALESCE(t2.first_missing_offset, t2.last_offset)-1
)
 SELECT kafka_partition,
        first_offset,
        MAX(kafka_offset) last_offset
   FROM (SELECT kafka_partition,
                kafka_offset,
                MAX(CASE
                     WHEN (kafka_offset - offset_prev) = 1 THEN NULL
                     ELSE kafka_offset
                    END) OVER (PARTITION BY kafka_partition ORDER BY kafka_offset ROWS BETWEEN unbounded preceding and current row) first_offset 
           FROM (SELECT kafka_partition,
                        kafka_offset,
                        lag (kafka_offset) OVER (PARTITION BY kafka_partition ORDER BY kafka_offset) offset_prev 
                   FROM events) s ) t
  GROUP BY t.kafka_partition,
           t.first_offset