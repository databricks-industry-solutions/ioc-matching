# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # How to do IOC matching using SQL
# MAGIC 
# MAGIC ## Working example
# MAGIC 
# MAGIC * We will use http data from PCAP extracted using Zeek and stored in `http` table. 
# MAGIC * The malicious IOCs stored in `ioc` table.
# MAGIC * Assume that we know and understand the schema of the data
# MAGIC * The working database/schema name is `ioc_matching_lipyeow_lim`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --INSERT INTO ioc_matching_lipyeow_lim.ioc VALUES ('ipv4', '192.168.202.103', '2012-03-16T16:00:00.000+0000', TRUE);

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM ioc_matching_lipyeow_lim.http
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM ioc_matching_lipyeow_lim.ioc
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## The simplest case: match the IOCs against the values of a single IPv4 column

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT d.*
# MAGIC FROM ioc_matching_lipyeow_lim.http AS d
# MAGIC   INNER JOIN ioc_matching_lipyeow_lim.ioc AS ioc ON d.id_orig_h=ioc.ioc_value AND ioc.active=TRUE

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## What if we want to match against multiple columns?

# COMMAND ----------

# DBTITLE 1,Make a list of observables from multiple columns
# MAGIC %sql
# MAGIC 
# MAGIC SELECT CONCAT(
# MAGIC         ARRAY(d.id_orig_h),
# MAGIC         ARRAY(d.id_resp_h)
# MAGIC       ) AS extracted_obslist
# MAGIC FROM ioc_matching_lipyeow_lim.http AS d

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## What if a column contains an IOC type as a substring (eg. http.referrer)?

# COMMAND ----------

# DBTITLE 1,Extract IOC types using regular expressions
# MAGIC %sql
# MAGIC 
# MAGIC SELECT d.referrer, regexp_extract_all(d.referrer, '(\\d+\\.\\d+\\.\\d+\\.\\d+)', 0) AS extracted_ipv4
# MAGIC FROM ioc_matching_lipyeow_lim.http AS d

# COMMAND ----------

# DBTITLE 1,Concat all extracted observables into an array
# MAGIC %sql
# MAGIC 
# MAGIC SELECT CONCAT(
# MAGIC         ARRAY(d.id_orig_h),
# MAGIC         ARRAY(d.id_resp_h),
# MAGIC         regexp_extract_all(d.referrer, '(\\d+\\.\\d+\\.\\d+\\.\\d+)', 0)
# MAGIC       ) AS extracted_obslist
# MAGIC FROM ioc_matching_lipyeow_lim.http AS d

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## IOC matching of an array of observables (v1 array_contains)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT aug.*
# MAGIC FROM 
# MAGIC   (
# MAGIC   SELECT d.*, CONCAT(
# MAGIC           ARRAY(d.id_orig_h),
# MAGIC           ARRAY(d.id_resp_h),
# MAGIC           regexp_extract_all(d.referrer, '(\\d+\\.\\d+\\.\\d+\\.\\d+)', 0)
# MAGIC         ) AS extracted_obslist
# MAGIC   FROM ioc_matching_lipyeow_lim.http AS d
# MAGIC   ) AS aug
# MAGIC   INNER JOIN ioc_matching_lipyeow_lim.ioc AS ioc ON ARRAY_CONTAINS(aug.extracted_obslist, ioc.ioc_value) AND ioc.active = TRUE  

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## IOC matching of an array of observables (v2 explode)

# COMMAND ----------

# DBTITLE 1,Explode array into a regular column
# MAGIC %sql
# MAGIC 
# MAGIC SELECT e.*, explode(e.extracted_obslist) AS extracted_obs
# MAGIC FROM 
# MAGIC   (
# MAGIC   SELECT 
# MAGIC --    d.*,
# MAGIC     d.ts, 
# MAGIC     to_json(struct(d.*)) AS raw, 
# MAGIC     CONCAT(
# MAGIC           ARRAY(d.id_orig_h),
# MAGIC           ARRAY(d.id_resp_h),
# MAGIC           regexp_extract_all(d.referrer, '(\\d+\\.\\d+\\.\\d+\\.\\d+)', 0)
# MAGIC       ) AS extracted_obslist
# MAGIC   FROM ioc_matching_lipyeow_lim.http AS d
# MAGIC   ) AS e

# COMMAND ----------

# DBTITLE 1,Use equi-join on the exploded column
# MAGIC %sql
# MAGIC 
# MAGIC SELECT now() AS detection_ts, aug.*
# MAGIC FROM 
# MAGIC   (
# MAGIC   SELECT e.*, explode(e.extracted_obslist) AS extracted_obs
# MAGIC   FROM
# MAGIC     (
# MAGIC     SELECT
# MAGIC       d.ts::timestamp,
# MAGIC       to_json(struct(d.*)) AS raw,
# MAGIC       CONCAT(
# MAGIC           ARRAY(d.id_orig_h),
# MAGIC           ARRAY(d.id_resp_h),
# MAGIC           regexp_extract_all(d.referrer, '(\\d+\\.\\d+\\.\\d+\\.\\d+)', 0)
# MAGIC         ) AS extracted_obslist
# MAGIC     FROM ioc_matching_lipyeow_lim.http AS d
# MAGIC     ) AS e
# MAGIC   ) AS aug
# MAGIC   INNER JOIN ioc_matching_lipyeow_lim.ioc AS ioc ON aug.extracted_obs=ioc.ioc_value AND ioc.active = TRUE  

# COMMAND ----------


