-- Databricks notebook source
CREATE STREAMING LIVE TABLE inc_iochits
AS
SELECT 
  now() AS detection_ts,
  ioc.ioc_value AS matched_ioc, 
  ioc.ioc_type, 
  aug.ts AS first_seen,
  aug.ts AS last_seen, 
  ARRAY('dns') AS src_tables, 
  ARRAY(aug.raw) AS raw 
FROM 
  (
  SELECT e.*, explode(e.extracted_obslist) AS extracted_obs
  FROM
    (
    SELECT
      d.ts::timestamp,
      to_json(struct(d.*)) AS raw,
      CONCAT(
          ARRAY(d.id_orig_h),
          ARRAY(d.id_resp_h),
          regexp_extract_all(d.referrer, '(\\d+\\.\\d+\\.\\d+\\.\\d+)', 0)
        ) AS extracted_obslist
    FROM stream(ioc_matching_lipyeow_lim.http) AS d
    ) AS e
  ) AS aug
  INNER JOIN ioc_matching_lipyeow_lim.ioc AS ioc ON aug.extracted_obs=ioc.ioc_value AND ioc.active = TRUE;

-- COMMAND ----------


