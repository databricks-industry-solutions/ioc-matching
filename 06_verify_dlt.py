# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/ioc-matching. 

# COMMAND ----------

# MAGIC %md
# MAGIC # Verify Delta Live Table Pipelines Capabilities
# MAGIC 
# MAGIC This notebook provides sample code for verifying the [Delta Live Table (DLT)](https://docs.databricks.com/data-engineering/delta-live-tables/index.html) pipelines for 
# MAGIC * continuous IOC matching (see `03_dlt_ioc_matching`) and
# MAGIC * maintaining the summary tables used for fast historical IOC search (see `04_dlt_summary_table`).
# MAGIC 
# MAGIC Note:
# MAGIC * you will need to manually modify the database/schema name used in those notebooks before you will be able to run them in your Databricks workspace. Please see the [DLT Quickstart](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-quickstart.html) guide on how to run DLT pipelines.
# MAGIC * the below instructions for the pre-requisite steps assumes a manual batch trigger approach to the DLT pipelines. You can also execute the DLTs in continuous mode.
# MAGIC 
# MAGIC ## Pre-requisites for Running the Verification Commands in this Notebook
# MAGIC 
# MAGIC 1. `02_ioc_matching` notebook successfully executed
# MAGIC 1. DLT pipeline for `03_dlt_ioc_matching` triggered with *full refresh* and executed successfully
# MAGIC 1. DLT pipeline for `04_dlt_summary_table` triggered with *full refresh* and executed successfully
# MAGIC 1. `05_insert_new_data` notebook executed successfully in order to insert new tuples
# MAGIC 1. DLT pipeline for `03_dlt_ioc_matching` triggered again and executed successfully
# MAGIC 1. DLT pipeline for `04_dlt_summary_table` triggered again and executed successfully

# COMMAND ----------

# DBTITLE 1,Setup shared configuration parameters
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Verify the DLT for Continuous/Incremental IOC Matching

# COMMAND ----------

# DBTITLE 1,Query hits matched by first execution of the DLT pipeline
verify_sql=f"""
SELECT *
FROM {getParam("inc_iochits_table")}
WHERE first_seen < '2022-03-17T00:00:00+00:00'
--AND detection_ts > now()-'3 MINUTES'::INTERVAL
"""
result_df = spark.sql(verify_sql)
print(f"{getParam('inc_iochits_table')} : result count before insert = {result_df.count()}")

# we will not use an assert here in order to allow for flexibility in the baseline use case

# COMMAND ----------

# DBTITLE 1,Query hits matched by second execution of the DLT pipeline post insert of new data
verify_sql=f"""
SELECT *
FROM {getParam("inc_iochits_table")}
WHERE first_seen > '2012-03-17T00:00:00+00:00'
"""
result_df = spark.sql(verify_sql)
print("-------------------------------------------")
print("IOC hits from the newly inserted DNS tuples")
print("-------------------------------------------")
display(result_df)

print(f"{getParam('inc_iochits_table')} : result count from insert = {result_df.count()}")
assert result_df.count()>=1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the DLT for IOC Summary Tables

# COMMAND ----------

# DBTITLE 1,Create Union-All View for Summary Tables
ddl = f"""
CREATE VIEW IF NOT EXISTS {getParam('db_name')}.ioc_summary_all
AS
SELECT * FROM {getParam('db_name')}.ioc_summary_dns
UNION ALL
SELECT * FROM {getParam('db_name')}.ioc_summary_http
"""

print(ddl)
spark.sql(ddl)

# COMMAND ----------

# DBTITLE 1,Query hits in summary tables prior to insert of new data
# This query retrieves the hits matched by the first execution of the DLT pipeline for ioc summary
verify_sql=f"""
SELECT *
FROM {getParam("db_name")}.ioc_summary_all
WHERE ts_day < '2012-03-17T00:00:00.000+0000'
AND obs_value='www.download.windowsupdate.com'
"""
result_df = spark.sql(verify_sql)
print(f"ioc_summary_all : result count before insert = {result_df.count()}")
display(result_df)

assert result_df.count()==2


# COMMAND ----------

# DBTITLE 1,Query hits in summary tables post insert of new data
verify_sql=f"""
SELECT *
FROM {getParam("db_name")}.ioc_summary_all
WHERE ts_day = '2022-04-18T00:00:00.000+0000'
  AND obs_value='10.10.117.209'
"""
result_df = spark.sql(verify_sql)
print(f"ioc_summary_all : result count after insert = {result_df.count()}")
display(result_df)

assert result_df.count()>=1

# COMMAND ----------


