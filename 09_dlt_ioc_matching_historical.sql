-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # DLT pipeline for continuous IOC matching in the presence of IOC updates
-- MAGIC 
-- MAGIC ## Prerequisites
-- MAGIC 
-- MAGIC * the `ioc_summary_all` view must created using the command in the notebook `08_handling_ioc_updates.sql`
-- MAGIC * the `v_logs_silver` view must created using the command in the notebook `08_handling_ioc_updates.sql`
-- MAGIC 
-- MAGIC ## Parametrization
-- MAGIC 
-- MAGIC In production, these DLT queries will be parametrized with the time window for the historical search over the summary tables and with the time window between the last update of the summary tables and now for the search over the recent log data.
-- MAGIC 
-- MAGIC ## Consumption of the results
-- MAGIC 
-- MAGIC The `summary_table_iochits` and the `recent_history_iochits` can be merged into a single view via union-all for a better user experience.

-- COMMAND ----------

CREATE STREAMING LIVE TABLE summary_table_iochits
AS
  SELECT
    now() AS detection_ts,
    s.obs_value AS matched_ioc,
    ioc.ioc_type, 
    min(s.first_seen) AS first_seen,
    max(s.last_seen) AS last_seen,
    collect_set(s.src_table) AS src_tables,
    collect_set(logs.raw) AS raw
  FROM STREAM(ioc_matching_lipyeow_lim.ioc) AS ioc 
    INNER JOIN ioc_matching_lipyeow_lim.ioc_summary_all AS s
    ON s.obs_value = ioc.ioc_value AND ioc.active = TRUE
    LEFT OUTER JOIN ioc_matching_lipyeow_lim.v_logs_silver AS logs 
    ON s.src_table = logs.src_table
      AND s.src_ip = logs.src_ip 
      AND s.dst_ip = logs.dst_ip 
      AND logs.ts >= s.first_seen
      AND logs.ts <= s.last_seen
  WHERE s.ts_day BETWEEN '2012-03-01T00:00:00+0000' AND '2012-04-01T00:00:00+0000'
  GROUP BY s.obs_value, ioc.ioc_type, s.src_ip, s.dst_ip
;


-- COMMAND ----------

CREATE STREAMING LIVE TABLE recent_history_iochits
AS
SELECT now() AS detection_ts, 
  ioc.ioc_value AS matched_ioc, 
  ioc.ioc_type, 
  min(timestamp(aug.ts)) AS first_seen,
  max(timestamp(aug.ts)) AS last_seen,
  collect_set(aug.src_table) AS src_tables, 
  collect_set(aug.raw) AS raw
FROM
  STREAM(ioc_matching_lipyeow_lim.ioc) AS ioc 
  INNER JOIN 
  (
  SELECT 'dns' AS src_table, exp.ts, exp.raw, extracted_obs
  FROM
    (
    SELECT d.ts, to_json(struct(d.*)) AS raw,
      concat(
        regexp_extract_all(d.query, '(\\d+\.\\d+\.\\d+\.\\d+)'),
        regexp_extract_all(d.id_orig_h, '(\\d+\.\\d+\.\\d+\.\\d+)'),
        regexp_extract_all(d.id_resp_h, '(\\d+\.\\d+\.\\d+\.\\d+)'),
        regexp_extract_all(d.query, '([\\w_-]+\.[\\w_-]+\.[\\w_-]+)$')
        ) AS extracted_obslist
    FROM ioc_matching_lipyeow_lim.dns AS d
    WHERE timestamp(d.ts) > '2012-03-01T00:00:00+0000'
    )  AS exp LATERAL VIEW explode(exp.extracted_obslist) AS extracted_obs 
  UNION ALL
  SELECT 'http' AS src_table, exp.ts, exp.raw, extracted_obs
  FROM
    (
    SELECT d.ts, to_json(struct(d.*)) AS raw,
      concat(
        regexp_extract_all(d.orig_filenames, '(\\d+\.\\d+\.\\d+\.\\d+)'),
        regexp_extract_all(d.orig_fuids, '(\\d+\.\\d+\.\\d+\.\\d+)'),
        regexp_extract_all(d.origin, '(\\d+\.\\d+\.\\d+\.\\d+)'),
        regexp_extract_all(d.resp_fuids, '(\\d+\.\\d+\.\\d+\.\\d+)'),
        regexp_extract_all(d.referrer, '(\\d+\.\\d+\.\\d+\.\\d+)'),
        regexp_extract_all(d.resp_filenames, '(\\d+\.\\d+\.\\d+\.\\d+)'),
        regexp_extract_all(d.resp_mime_types, '(\\d+\.\\d+\.\\d+\.\\d+)'),
        regexp_extract_all(d.id_orig_h, '(\\d+\.\\d+\.\\d+\.\\d+)'),
        regexp_extract_all(d.host, '(\\d+\.\\d+\.\\d+\.\\d+)'),
        regexp_extract_all(d.id_resp_h, '(\\d+\.\\d+\.\\d+\.\\d+)'),
        regexp_extract_all(d.orig_mime_types, '(\\d+\.\\d+\.\\d+\.\\d+)'),
        regexp_extract_all(d.referrer, '([\\w_-]+\.[\\w_-]+\.[\\w_-]+)$')
        ) AS extracted_obslist
    FROM ioc_matching_lipyeow_lim.http AS d
    WHERE timestamp(d.ts) > '2012-03-01T00:00:00+0000'
    )  AS exp LATERAL VIEW explode(exp.extracted_obslist) AS extracted_obs
  ) AS aug 
  ON aug.extracted_obs=ioc.ioc_value AND ioc.active = TRUE
  GROUP BY detection_ts, matched_ioc, ioc_type, date_trunc('DAY', timestamp(aug.ts))
;

-- COMMAND ----------


