-- Databricks notebook source
-- MAGIC %md 
-- MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/ioc-matching. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Delta Live Table Pipeline for Continuous IOC Matching
-- MAGIC 
-- MAGIC * Note that this SQL is generated from the main notebook and copy-pasted below
-- MAGIC * You will need to modify the database/schema name in order for the SQL to work
-- MAGIC * When you create the DLT pipeline (see [Quickstart](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-quickstart.html)), you should also specify the target database/schema to ioc_matching_{your sql compatible username}

-- COMMAND ----------

CREATE STREAMING LIVE TABLE inc_iochits
AS
SELECT now() AS detection_ts, ioc.ioc_value AS matched_ioc, ioc.ioc_type, aug.ts AS first_seen, aug.ts AS last_seen, ARRAY('dns') AS src_tables, ARRAY(aug.raw) AS raw 
FROM
  (
  SELECT timestamp(exp.ts) AS ts, exp.raw, extracted_obs
  FROM
    (
    SELECT d.ts, to_json(struct(d.*)) AS raw,
      concat(
        regexp_extract_all(d.query, '(\\d+\.\\d+\.\\d+\.\\d+)'),
        regexp_extract_all(d.id_orig_h, '(\\d+\.\\d+\.\\d+\.\\d+)'),
        regexp_extract_all(d.id_resp_h, '(\\d+\.\\d+\.\\d+\.\\d+)'),
        regexp_extract_all(d.query, '([\\w_-]+\.[\\w_-]+\.[\\w_-]+)$')
        ) AS extracted_obslist
    FROM stream(ioc_matching_lipyeow_lim.dns) AS d
    )  AS exp LATERAL VIEW explode(exp.extracted_obslist) AS extracted_obs
  ) AS aug 
  INNER JOIN ioc_matching_lipyeow_lim.ioc AS ioc ON aug.extracted_obs=ioc.ioc_value AND ioc.active=TRUE

UNION ALL

SELECT now() AS detection_ts, ioc.ioc_value AS matched_ioc, ioc.ioc_type, aug.ts AS first_seen, aug.ts AS last_seen, ARRAY('http') AS src_tables, ARRAY(aug.raw) AS raw
FROM
  (
  SELECT timestamp(exp.ts) AS ts, exp.raw, extracted_obs
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
    FROM stream(ioc_matching_lipyeow_lim.http) AS d
    )  AS exp LATERAL VIEW explode(exp.extracted_obslist) AS extracted_obs
  ) AS aug 
  INNER JOIN ioc_matching_lipyeow_lim.ioc AS ioc ON aug.extracted_obs=ioc.ioc_value AND ioc.active=TRUE
;

-- COMMAND ----------


