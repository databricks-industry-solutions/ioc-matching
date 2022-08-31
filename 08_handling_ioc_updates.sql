-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Continuous IOC matching with continuous updates of IOCs
-- MAGIC 
-- MAGIC ## Use Case
-- MAGIC 
-- MAGIC Enterprises want to automatically perform IOC matching of all their cybersecurity log and telemetry data against a curated set of IOCs (IPv4s, FQDNs, hashes) as the IOCs are updated automatically or semi-automatically by analysts or by subscribed threat intelligence services.
-- MAGIC 
-- MAGIC ## Overview
-- MAGIC 
-- MAGIC In many enterprises with a mature cybersecurity practice, there is a capability to centrally manage all the IOCs relevant to the enterprise. This typically involves collating the IOCS from several sources:
-- MAGIC * Threat intelligence subscriptions
-- MAGIC * IOCs discovered by the internal SOC team from the actual incidents
-- MAGIC * IOCs discovered by threat hunting teams or threat research teams from various other sources 
-- MAGIC 
-- MAGIC In this solution accelerator, we model the curated set of IOCs using the `ioc` table and abstract out the system that manages the lifecycle of the IOCs (that topic requires a separate treatise). The `ioc` table is therefore not static, but is continuously updated with the addition of newly discovered IOCs or with the deactivation of IOCs that are no longer relevant. Note that the deactivation of IOCs is a critical component of the lifecycle of the curated IOCs in order to manage the quantity and the quality of the curated IOCs.
-- MAGIC 
-- MAGIC ![usecase_image](https://raw.githubusercontent.com/lipyeowlim/public/main/img/ioc-matching/streaming-ioc-matching.png)
-- MAGIC 
-- MAGIC 
-- MAGIC There are two cases to handle when attempting to perform continuous IOC matching in the presence of these IOC lifecycle updates:
-- MAGIC 1. Log data that arrives after IOC updates
-- MAGIC 2. Log data that arrived before IOC updates and hence are already ingested
-- MAGIC 
-- MAGIC We will be combining and extending the streaming DLT concept and the summary table concept that we have introduced in the notebooks `03_dlt_ioc_matching.sql` and `04_dlt_summary_table.sql` to address the two cases.
-- MAGIC 
-- MAGIC ## Case 1: Log data that arrives after IOC updates
-- MAGIC 
-- MAGIC The main concern in this case is to ensure that the newly arrived data is matched against the latest IOCs. This case is addressed by the continuous IOC matching Delta Live Tables (DLT) pipeline using the notebook `03_dlt_ioc_matching.sql`. If you are using scheduled batch jobs, the updated `ioc` table will be picked up and used in the ioc matching join query in the next execution of the notebook on the newly arrived log data. If you are using streaming/continuous jobs, the ioc matching query will pick up the latest snapshot of the `ioc` table at the next micro-batch execution. See https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-incremental-data.html#streaming-joins.
-- MAGIC 
-- MAGIC ## Case 2: Log data that arrived before IOC updates
-- MAGIC 
-- MAGIC The main concern in this case is to ensure that any new IOCs is checked against all the historical data that was previously ingested. This case is best addressed by the summary tables that are built for the historical data. The notebook `04_dlt_summary_table.sql` provides the sample logic to construct such summary tables via DLT pipelines. With the summary tables, you would then create a streaming DLT query where each newly added IOC gets matched against the summary tables - provided in `09_dlt_ioc_matching_historical.sql`. This streaming DLT query can be run as a batch job or as a streaming job. Naturally, batch jobs will be more cost efficient than streaming jobs. For the batch job approach, the batch schedule and the time granularity of the summary table DLT pipeline may create a blind spot in terms of time coverage. For example, 
-- MAGIC 1. your `ioc` table is updated sporadically throughout the day,
-- MAGIC 2. your summary tables are updated daily at midnight,
-- MAGIC 2. your ioc matching batch job (against the summary tables) runs daily at 0600 hours,
-- MAGIC any new IOC that is added to the `ioc` table after 0600 hours will not get matched against the historical data until 0600 hours the following day. If that delay does not meet your operational requirements, you have the option to perform IOC matching of newly arrived IOCs (either stream-based on batch-based) against the sliver of actual historical data (not the summary tables) in between the job for ioc matching against the summary tables. The `09_dlt_ioc_matching_historical.sql` notebook contains sample streaming DLT SQL query for this sub-case as well. You can tune the frequency of these jobs according to your time-coverage requirements and resources required. 
-- MAGIC 
-- MAGIC ![usecase_image](https://raw.githubusercontent.com/lipyeowlim/public/main/img/ioc-matching/streaming-time-coverage.png)
-- MAGIC 
-- MAGIC Even with the streaming job approach where, conceptually, each newly added IOC is matched against the data in the summary tables, there will be a blind spot where the historical data between the last update of the summary tables and the current time are not being checked. Again, a separate job that checks the newly added IOC against the actual log data (not the summary tables) between the last update of the summary tables and the current time can be used to mitigate this gap in time coverage if needed. The `09_dlt_ioc_matching_historical.sql` notebook contains sample streaming DLT SQL query for this sub-case as well.
-- MAGIC 
-- MAGIC ## Streaming interpretation
-- MAGIC 
-- MAGIC A streaming interpretation is very useful in summarizing the two cases. In the first case, we treat the log data as streaming data sources and incrementally match new log data records against a snapshot of the latest `ioc` table. In the second case, we treat the `ioc` table as a stream (or change data capture stream) and match each new IOCs against the summary tables that represents the historical data up to a the last summary table update time. 
-- MAGIC 
-- MAGIC ## Outline of this notebook
-- MAGIC 
-- MAGIC 1. Print out summary table size statistics to understand the (lossy) compression ratios
-- MAGIC 1. Create the various union-all views that would greatly simplify the following SQL queries and user experience
-- MAGIC 1. Show how to query the summary tables using a time window filter and format the results in a user-friendly way
-- MAGIC 1. Insert a new IOC into the `ioc` table to simulate a newly added IOC.
-- MAGIC 1. Show how to perform IOC matching against the summary tables - this query will be the basis for the DLT for case (2) above. The time filter on `ioc.created_ts` is used to simulate the processing on the newly created IOCs. The time filter on `ioc_summary_all.ts_day` is used to specify the time range for the historical search. 
-- MAGIC 1. Show how to retrieve the underlying raw data for the results of the IOC matching against summary tables - this will be needed by analysts investigating the hits from the IOC matching against the summary tables
-- MAGIC 1. Show how to perform IOC matching of newly added IOCs against recent log data not covered by the summary tables. The time filter on the `ts` field of the log data is used to narrow the search to only the data not covered by the summary tables.
-- MAGIC 
-- MAGIC ## Prerequisites to running this notebook
-- MAGIC 
-- MAGIC 1. Run the `02_ioc_matching.py` notebook.
-- MAGIC 2. Set up a DLT pipeline job using `04_dlt_summary_table.sql` and run the DLT job. 

-- COMMAND ----------

-- DBTITLE 1,Statistics of summary table sizes
SELECT 
(SELECT count(*) FROM ioc_matching_lipyeow_lim.dns) AS dns_cnt,
(SELECT count(*) FROM ioc_matching_lipyeow_lim.ioc_summary_dns) AS dns_summary_cnt,
(SELECT count(*) FROM ioc_matching_lipyeow_lim.http) AS http_cnt,
(SELECT count(*) FROM ioc_matching_lipyeow_lim.ioc_summary_http) AS http_summary_cnt
;

-- COMMAND ----------

-- DBTITLE 1,Create the UNION-ALL view for all the summary tables
DROP VIEW IF EXISTS ioc_matching_lipyeow_lim.ioc_summary_all
;
CREATE VIEW IF NOT EXISTS ioc_matching_lipyeow_lim.ioc_summary_all
AS 
SELECT 'dns' AS src_table, d.*
FROM ioc_matching_lipyeow_lim.ioc_summary_dns AS d
UNION ALL
SELECT 'http' AS src_table, h.*
FROM ioc_matching_lipyeow_lim.ioc_summary_http AS h
;

-- COMMAND ----------

-- DBTITLE 1,Ad hoc time range filtering on summary tables
SELECT obs_value, src_ip, dst_ip, sum(cnt) AS cnt, min(first_seen) AS first_seen, max(last_seen) AS last_seen, collect_set(src_table) AS src_tables
FROM ioc_matching_lipyeow_lim.ioc_summary_all
WHERE ts_day BETWEEN '2012-03-01T00:00:00+0000' AND '2012-04-01T00:00:00+0000'
GROUP BY obs_value, src_ip, dst_ip
ORDER BY cnt DESC
;

-- COMMAND ----------

INSERT INTO ioc_matching_lipyeow_lim.ioc VALUES ('ipv4', '44.206.168.192', '2022-08-29T00:00:00+0000', TRUE);

-- COMMAND ----------

-- DBTITLE 1,IOC matching against summary tables with time range filter
SELECT s.obs_value, ioc.ioc_type, s.src_ip, s.dst_ip, sum(s.cnt) AS cnt, min(s.first_seen) AS first_seen, max(s.last_seen) AS last_seen, collect_set(s.src_table) AS src_tables
FROM ioc_matching_lipyeow_lim.ioc_summary_all AS s
INNER JOIN ioc_matching_lipyeow_lim.ioc AS ioc ON s.obs_value = ioc.ioc_value AND ioc.active = TRUE AND ioc.created_ts > '2022-08-28T00:00:00+0000'
WHERE s.ts_day BETWEEN '2012-03-01T00:00:00+0000' AND '2012-04-01T00:00:00+0000'
GROUP BY s.obs_value, ioc.ioc_type, s.src_ip, s.dst_ip
ORDER BY cnt DESC
;

-- COMMAND ----------

DROP VIEW IF EXISTS ioc_matching_lipyeow_lim.v_logs_silver
;

CREATE VIEW IF NOT EXISTS ioc_matching_lipyeow_lim.v_logs_silver
AS
SELECT 'dns' AS src_table, TIMESTAMP(d.ts) AS ts, d.id_orig_h AS src_ip, d.id_resp_h AS dst_ip, to_json(STRUCT(d.*)) AS raw
FROM ioc_matching_lipyeow_lim.dns AS d
UNION ALL
SELECT 'http' AS src_table, TIMESTAMP(h.ts) AS ts, h.id_orig_h AS src_ip, h.id_resp_h AS dst_ip, to_json(STRUCT(h.*)) AS raw
FROM ioc_matching_lipyeow_lim.http AS h
;

-- COMMAND ----------

-- DBTITLE 1,If you still have the raw data, you can retrieve the potentially matching raw data as well
SELECT
    now() AS detection_ts,
    s.obs_value AS matched_ioc,
    ioc.ioc_type, 
    min(s.first_seen) AS first_seen,
    max(s.last_seen) AS last_seen,
    collect_set(s.src_table) AS src_tables,
    collect_set(logs.raw) AS raw
FROM ioc_matching_lipyeow_lim.ioc AS ioc 
  INNER JOIN ioc_matching_lipyeow_lim.ioc_summary_all AS s
    ON s.obs_value = ioc.ioc_value AND ioc.active = TRUE AND ioc.created_ts > '2022-08-28T00:00:00+0000'
  LEFT OUTER JOIN ioc_matching_lipyeow_lim.v_logs_silver AS logs 
    ON s.src_table = logs.src_table
      AND s.src_ip = logs.src_ip 
      AND s.dst_ip = logs.dst_ip 
      AND logs.ts >= s.first_seen
      AND logs.ts <= s.last_seen
WHERE s.ts_day BETWEEN '2012-03-01T00:00:00+0000' AND '2012-04-01T00:00:00+0000'
GROUP BY s.obs_value, ioc.ioc_type, s.src_ip, s.dst_ip;

-- COMMAND ----------

-- DBTITLE 1,Alternate SQL if you want to preserve the total match counts from the summary tables
WITH matches AS 
(
  SELECT s.obs_value, ioc.ioc_type, s.src_ip, s.dst_ip, 
    sum(s.cnt) AS cnt,
    min(s.first_seen) AS first_seen,
    max(s.last_seen) AS last_seen,
    collect_set(s.src_table) AS src_tables
  FROM ioc_matching_lipyeow_lim.ioc_summary_all AS s
    INNER JOIN ioc_matching_lipyeow_lim.ioc AS ioc 
    ON s.obs_value = ioc.ioc_value 
      AND ioc.active = TRUE 
      AND ioc.created_ts > '2022-08-28T00:00:00+0000'
  WHERE s.ts_day BETWEEN '2012-03-01T00:00:00+0000' AND '2012-04-01T00:00:00+0000'
  GROUP BY s.obs_value, ioc.ioc_type, s.src_ip, s.dst_ip
)
SELECT matches.obs_value,
  matches.ioc_type,
  first(matches.cnt) AS match_cnt,
  first(matches.first_seen) AS first_seen,
  first(matches.last_seen) AS last_seen,
  first(matches.src_tables) AS src_tables,
  collect_set(logs.raw) AS raw
FROM matches LEFT OUTER JOIN ioc_matching_lipyeow_lim.v_logs_silver AS logs 
    ON array_contains(matches.src_tables, logs.src_table)
      AND matches.src_ip = logs.src_ip 
      AND matches.dst_ip = logs.dst_ip 
      AND logs.ts >= matches.first_seen
      AND logs.ts <= matches.last_seen
GROUP BY matches.obs_value, matches.ioc_type, matches.src_ip, matches.dst_ip
;


-- COMMAND ----------

-- DBTITLE 1,Sample query for matching newly added IOCs against recent logs not covered by summary tables
SELECT now() AS detection_ts, 
  ioc.ioc_value AS matched_ioc, 
  ioc.ioc_type, 
  min(timestamp(aug.ts)) AS first_seen,
  max(timestamp(aug.ts)) AS last_seen,
  collect_set(aug.src_table) AS src_tables, 
  collect_set(aug.raw) AS raw
FROM
  ioc_matching_lipyeow_lim.ioc AS ioc 
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
  ON aug.extracted_obs=ioc.ioc_value AND ioc.active = TRUE AND ioc.created_ts > '2022-08-28T00:00:00+0000'
  GROUP BY detection_ts, matched_ioc, ioc_type, date_trunc('DAY', timestamp(aug.ts))
;

-- COMMAND ----------

SELECT min(timestamp(ts)), max(timestamp(ts))
FROM ioc_matching_lipyeow_lim.dns;

-- COMMAND ----------

select array('hello');

-- COMMAND ----------


