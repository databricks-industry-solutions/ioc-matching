# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # How to automate IOC matching on data ingest
# MAGIC 
# MAGIC How do we automatically perform IOC matching during data ingestion and store matches into an `iochits` table?
# MAGIC 
# MAGIC The `iochits` table can be consumed by threat hunters, soc analysts or fed into SIEMs, BI tools, messaging tools (eg. slack).

# COMMAND ----------

# DBTITLE 1,From episode 001
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

# MAGIC %md
# MAGIC 
# MAGIC ## How do we automate this IoC matching query?
# MAGIC 
# MAGIC * Create a SQL notebook with a DLT "query" (see `03b_dlt_auto_ioc_matching.sql`)
# MAGIC * Add prefix `CREATE LIVE TABLE auto_iochits AS` to the SQL query above.
# MAGIC * Create a DLT job and run it on a schedule (or continuously)

# COMMAND ----------

# DBTITLE 1,Let's check the auto_iochits table
# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM ioc_matching_lipyeow_lim.auto_iochits;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## How do we make this automated ioc matching incremental?
# MAGIC 
# MAGIC * Use prefix `CREATE STREAMING LIVE TABLE inc_iochits AS` instead (note the `STREAMING` keyword)
# MAGIC * Enclose the input data table with `stream(...)` for incremental matching otherwise the query will run over entire table (see `03b_dlt_auto_ioc_matching.sql`)
# MAGIC * Create and run this DLT job

# COMMAND ----------

# DBTITLE 1,Let's check the inc_iochits table
# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM ioc_matching_lipyeow_lim.inc_iochits;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Insert 1 new row into the `http` table to simulate data ingestion

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- unix timestamp 1650240000 translates to "2022-04-18T00:00:00.000+0000"
# MAGIC INSERT INTO ioc_matching_lipyeow_lim.http VALUES
# MAGIC (1650240000.0,'CxJL003hN0bnRi5xtg','192.168.202.76','52227','192.168.229.156','80','1','GET','192.168.229.156','/admin/config.php?type=tool&display=index&quietmode=1&info=info&restrictmods=core/dashboard','http://192.168.229.156/admin/config.php','1.1','Mozilla/5.0 (Windows NT 6.1; WOW64; rv:10.0.2) Gecko/20100101 Firefox/10.0.2','-','0','3502','200','OK','-','-','(empty)','admin','-','-','-','-','-','FXT8oG4p7QQLof1Zsf','-','text/json')
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Trigger the DLT job and check the results

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM ioc_matching_lipyeow_lim.inc_iochits;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## How do we handle multiple data sources or tables?
# MAGIC 
# MAGIC * Option 1: Use `UNION ALL` within the `CREATE STREAMING LIVE TABLE inc_iochits ...` ddl.
# MAGIC * Option 2: Use separate `CREATE STREAMING LIVE TABLE inc_iochits_http` ddls for each source table and create a view to union all the resulting `inc_iochits_*` tables. 

# COMMAND ----------


