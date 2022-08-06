-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Build summary tables using Delta Live Tables
-- MAGIC 
-- MAGIC * Summary tables are lossy, aggregated IOC occurrence information (essentially a materialized view) constructed to speed up ad hoc historical IOC matching.
-- MAGIC * You will need to modify the database/schema name in order for the SQL to work
-- MAGIC * When you create the DLT pipeline (see [Quickstart](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-quickstart.html)), you should also specify the target database/schema to ioc_matching_{your sql compatible username}

-- COMMAND ----------

CREATE STREAMING LIVE TABLE ioc_summary_dns
AS
SELECT ts_day, obs_value, src_data, src_ip, dst_ip, count(*) AS cnt
FROM
  (
  SELECT 'dns' AS src_data, extracted_obs AS obs_value, date_trunc('DAY', timestamp(exp.ts)) as ts_day, exp.id_orig_h as src_ip, exp.id_resp_h as dst_ip
  FROM
    (
    SELECT d.*,
      concat(
        regexp_extract_all(d.query, '(\\d+\\.\\d+\\.\\d+\\.\\d+)', 0),
        ARRAY(d.id_orig_h),
        ARRAY(d.id_resp_h),
        regexp_extract_all(d.query, '((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}', 0)
        ) AS extracted_obslist
    FROM stream(ioc_matching_lipyeow_lim.dns) AS d
    )  AS exp LATERAL VIEW explode(exp.extracted_obslist) AS extracted_obs
  ) AS aug
GROUP BY ts_day, obs_value, src_data, src_ip, dst_ip;

CREATE STREAMING LIVE TABLE ioc_summary_http
AS
SELECT ts_day, obs_value, src_data, src_ip, dst_ip, count(*) AS cnt
FROM
  (
  SELECT 'http' AS src_data, extracted_obs AS obs_value, date_trunc('DAY', timestamp(exp.ts)) as ts_day, exp.id_orig_h as src_ip, exp.id_resp_h as dst_ip
  FROM
    (
    SELECT d.*,
      concat(
        regexp_extract_all(d.referrer, '(\\d+\\.\\d+\\.\\d+\\.\\d+)', 0),
        ARRAY(d.id_orig_h),
        ARRAY(d.host),
        ARRAY(d.id_resp_h),
        regexp_extract_all(d.referrer, '((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}', 0)
        ) AS extracted_obslist
    FROM stream(ioc_matching_lipyeow_lim.http) AS d
    )  AS exp LATERAL VIEW explode(exp.extracted_obslist) AS extracted_obs
  ) AS aug
GROUP BY ts_day, obs_value, src_data, src_ip, dst_ip;


-- COMMAND ----------


