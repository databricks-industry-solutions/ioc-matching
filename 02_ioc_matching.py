# Databricks notebook source
# MAGIC %md
# MAGIC # Ad Hoc IOC Matching on All Tables
# MAGIC 
# MAGIC version 2.0 (2022-08-30)
# MAGIC 
# MAGIC ![usecase_image](https://raw.githubusercontent.com/lipyeowlim/public/main/img/ioc-matching/ir-ioc-matching.png)
# MAGIC 
# MAGIC ## Use cases covered by the notebooks in this solution accelerator
# MAGIC 
# MAGIC * **Schema-agnostic IOC matching scan**: During an incident response (IR) engagement, an analyst might want to perform an ad hoc scan of all the data (logs, telemetry, etc.) in a security lakehouse for a given list of atomic Indicators-of-Compromise (IOCs) without the need to have deep understanding of the table schemas. The `02_ioc_matching` notebook addresses this use case.
# MAGIC * **Continuous IOC matching**: The approach in the `02_ioc_matching` notebook can be easily adapted to perform incremental or continuous IOC matching using [Delta Live Tables (DLT)](https://docs.databricks.com/data-engineering/delta-live-tables/index.html). An example is given in the `03_dlt_ioc_matching` notebook.
# MAGIC * **Ad hoc historical IOC search**: Historical IOC search at interactive speeds can be done using summary tables constructed using DLT. An example is given in the `04_dlt_summary_table` notebook. The `06_verify_dlt` notebook provides a series of steps to verify the DLT capabilities.
# MAGIC * **Multi-cloud/region federated query**: Log ingestion and IOC matching can happen in each cloud or region without incurring egress costs. Hunting and triage of IOC hits can use federated queries from a single workspace to get results back from the workspaces in each cloud or region. The `07_multicloud` notebook demonstrates the use of multi-cloud and multi-region federated queries. 
# MAGIC * **Fully-automated continuous IOC matching with continuous IOC updates**: The streaming IOC matching approach in the `03_dlt_ioc_matching` notebook and the summary table approach in the `04_dlt_summary_table` notebook can be combined and extended to fully automate the IOC matching process even when the curated set of IOCs are constantly updated. In particular, when a new IOC is added, not only should newly ingested log data be matched against the new IOC, but the historical data needs to be matched against the new IOC. The `08_handling_ioc_updates` notebook demonstrates these concepts.
# MAGIC 
# MAGIC ## Overview of this Notebook
# MAGIC * Setup: Initialize configuration parameters, create database and load three delta tables: `dns`, `http`, `ioc`
# MAGIC * **Step 1**: Scan all table schemas/metadata in the Databricks workspace
# MAGIC * **Step 2**: Infer fields that might contain IOC/observables (eg. IPv4, IPv6, fqdn, MD5, SHA1, SHA256)
# MAGIC * **Step 3**: Run join queries for those fields against a table of known bad IOCs. The resulting matches are stored in the `iochits` table to be fed into a downstream SIEM/SOAR and/or triaged directly by analysts.
# MAGIC * Generate SQL DLT pipeline for continuous/incremental IOC matching pipeline
# MAGIC * Generate SQL DLT pipeline for maintaining summary tables for fast historical IOC search
# MAGIC 
# MAGIC ## Value
# MAGIC 
# MAGIC * Simplicity: agility & operational effectiveness
# MAGIC * Schema-agnostic: variety of data sources during IR
# MAGIC * Performance: Large historical time window
# MAGIC 
# MAGIC ## Running this notebook
# MAGIC 
# MAGIC * If you received a DBC file for this solution accelerator, you just need to import the DBC file
# MAGIC * If you received a zip file for this solution accelerator, the easiest way to get started is to:
# MAGIC   * Create a git repo
# MAGIC   * Unzip and copy the code into the git repo
# MAGIC   * Add the repo in the Databricks console -> Repos -> "Add Repo"
# MAGIC   * Once the repo is added, you can go to the repo and click on the `02_ioc_matching` notebook.
# MAGIC * Note that only the dns & http sample data are included in the notebook. The `vpc_flow_logs` database is not included in the notebook.

# COMMAND ----------

# DBTITLE 1,Setup environment feature flag
dbutils.widgets.dropdown("env", "test", ["test", "demo"])
env = dbutils.widgets.get("env")

# COMMAND ----------

# DBTITLE 1,Run configuration notebook
# MAGIC %run ./01_setup_test_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create a temporary view of the database/table/column metadata 
# MAGIC 
# MAGIC * Respect the included & excluded constraints
# MAGIC * Display the view for preview

# COMMAND ----------

# construct a temporary view/table of columns
# these table metadata are relatively small, so it is ok to do collect()

if type(getParam("included_databases"))==list:
  db_list = getParam("included_databases")
elif type(getParam("included_databases"))==string and getParam("included_databases") in ['all', '*']:
  # caution: this option is extremely compute intensive
  db_list = [x[0] for x in spark.sql("SHOW DATABASES").collect()]
else:
  raise Exception("Invalid value for included_databases param.")

# full list = schema, table, column, type
full_list = []
for i in db_list:
  try:
    tb_df = spark.sql(f"SHOW TABLES IN {i}")
  except Exception as x:
    print(f"Unable to show tables in {i} ... skipping")
    continue
  for (db, table_name, is_temp) in tb_df.collect():
    full_table_name = db + "." + table_name
    if is_temp or full_table_name in getParam("excluded_tables"):
      continue
    try:
      cols_df = spark.sql(f"DESCRIBE {full_table_name}")
    except Exception as x:
      # most likely the exception is a permission denied, coz the table is not visible to this user account
      print(f"Unable to describe {full_table_name} ... skipping")
      continue
    for (col_name, col_type, comment) in cols_df.collect():
      if not col_type or col_name[:5]=="Part ":
        continue
      full_list.append([db, table_name, col_name, col_type])  
    
spark.createDataFrame(full_list, schema = ['database', 'tableName', 'columnName', 'colType']).createOrReplaceTempView("allColumns")

display(spark.sql("SELECT * FROM allColumns"))

# COMMAND ----------

# DBTITLE 1,[Optional:] Show statistics of the included databases
db_stats_sql = """
SELECT c.database, count(distinct c.tableName) AS num_of_tables
FROM allColumns as c
GROUP BY c.database;
"""

col_stats_sql = """
  SELECT c.database || '.' || c.tableName AS table_name, count(c.columnName) AS num_of_cols
  FROM allColumns as c
  GROUP BY c.database, c.tableName
"""

display( spark.sql(db_stats_sql) )

sql_list = [ f"(SELECT '{db}.{tb}' AS table_name, count(*) AS num_of_rows FROM {db}.{tb})" for (db, tb) in spark.sql("SELECT DISTINCT database, tableName FROM allColumns").collect() ]
tb_stats_sql = f"""
SELECT R.table_name, C.num_of_cols, format_number(R.num_of_rows, 0) as num_of_rows
FROM
  ({" union ".join(sql_list)}) AS R 
  INNER JOIN ({col_stats_sql}) AS C ON R.table_name = C.table_name
"""

display( spark.sql(tb_stats_sql) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Find candidate columns for IOC matching
# MAGIC 
# MAGIC * The following SQL query identifies candidate columns for IOC matching based on (sub-)string matching on column names.
# MAGIC * More sophisticated criteria are possible. 
# MAGIC   * For example, a sample of the values in each column can be used to determine if the column is likely to contain an IP address or fqdn etc.
# MAGIC   * ML-based classifier
# MAGIC * Candidate columns are split into different IOC categories in order to allow different extraction functions to be applied before matching.

# COMMAND ----------

# This query should find all columns that have a high likelihood of containing an IP address or an FQDN or a hash (md5/sha1/sha256).
# We will ignore non-String and complex types for now even though IP addresses can sometimes be encoded as integer type
# TODO: to handle complex types, we will stringify the value (eg. to_json()) 
# The where-clause for each category will get built out by analysts over time. 
metadata_sql_str = """
SELECT database, tableName, 
  collect_set(columnName) FILTER 
            (WHERE columnName ilike '%orig%' 
            OR columnName ilike '%resp%'
            OR columnName ilike '%dest%'
            OR columnName ilike '%dst%'
            OR columnName ilike '%src%' 
            OR columnName ilike '%ipaddr%'
            OR columnName IN ( 'query', 'host', 'referrer' )) AS ipv4_col_list,
  collect_set(columnName) FILTER
            (WHERE columnName IN ('query', 'referrer')) AS fqdn_col_list
FROM allColumns
WHERE colType='string'
GROUP BY database, tableName
"""

display(spark.sql(metadata_sql_str))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 3: Run IOC matching on all tables
# MAGIC 
# MAGIC * The extraction logic relies on regular expression and the Databricks SQL built-in `function regexp_extract_all()`. The sample regexp below should be built out according to specific scenarios. 
# MAGIC * Custom extraction logic can be supported using a user-defined function (UDF) with some performance degradation. The UDF must return an array of strings.
# MAGIC * For each table in the included databases (except the excluded tables), we generate the SQL for doing the IOC extraction from the candidate columns and the matching against the IOC table.
# MAGIC * Each SQL query is run sequentially in this demo, but could be run asynchronously in parallel in production.
# MAGIC * The IOC matches are appended into an `iochits` table for review & triage by security analysts and/or for ingestion into downstream SIEM/SOAR systems.

# COMMAND ----------

# DBTITLE 1,Define function for SQL generation
def genSQL(db, tb, ipv4_col_list, fqdn_col_list, ioc_table_name, dlt=False):
  full_table_name = db + "." + tb
  optimizer_hint = " /*+ BROADCAST(ioc) */ "
  if dlt:
    optimizer_hint = ""
    full_table_name = "stream(" + full_table_name + ")"
  # this sample regexp does not check if the octets are 0-255
  ioc_extract_str_list = [ f"regexp_extract_all(d.{c}, '(\\\\d+\\\\.\\\\d+\\\\.\\\\d+\\\\.\\\\d+)', 0)" for c in ipv4_col_list ]
  # this sample regexp was adapted from https://mkyong.com/regular-expressions/domain-name-regular-expression-example/
  ioc_extract_str_list.extend([ f"regexp_extract_all(d.{c}," + " '((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\\\.)+[A-Za-z]{2,6}', 0)" for c in fqdn_col_list ])
  ioc_str = "concat(\n        " + ",\n        ".join(ioc_extract_str_list) + "\n        ) AS extracted_obslist"
  
  ioc_match_sql_str = f'''
SELECT {optimizer_hint} now() AS detection_ts, ioc.ioc_value AS matched_ioc, ioc.ioc_type, aug.ts AS first_seen, aug.ts AS last_seen, 
  ARRAY('{full_table_name}') AS src_tables, ARRAY(aug.raw) AS raw
FROM
  (
  SELECT timestamp(exp.ts) AS ts, exp.raw, extracted_obs
  FROM
    (
    SELECT d.ts, to_json(struct(d.*)) AS raw,
      {ioc_str}
    FROM {full_table_name} AS d
    )  AS exp LATERAL VIEW explode(exp.extracted_obslist) AS extracted_obs
  ) AS aug 
  INNER JOIN {ioc_table_name} AS ioc ON aug.extracted_obs=ioc.ioc_value AND ioc.active=TRUE
  '''
  return ioc_match_sql_str

# COMMAND ----------

# DBTITLE 1,Run the generated SQL
for (db, tb, ipv4_col_list, fqdn_col_list) in spark.sql(metadata_sql_str).collect():
  sql_str = genSQL(db, tb, ipv4_col_list, fqdn_col_list, getParam("ioc_table"))
  print(f"\n----\n{sql_str}")
  df = spark.sql(sql_str)
  df.write.format("delta").mode("append").saveAsTable(getParam("iochits_table"))
  
display(spark.sql(f"SELECT * FROM {getParam('iochits_table')}"))
  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continuous IOC matching using delta live tables
# MAGIC 
# MAGIC If you would like to convert this ad hoc historical IOC matching capability into a continuous/incremental monitoring pipeline, you can use the Delta Live Table (DLT) feature:
# MAGIC * Run the following command to generate the SQL for the DLT pipeline and copy the SQL
# MAGIC * Follow the [DLT quick start guide](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-quickstart.html) and paste the SQL into the SQL notebook for the DLT pipeline

# COMMAND ----------

sql_list = []
for (db, tb, ipv4_col_list, fqdn_col_list) in spark.sql(metadata_sql_str).collect():
  sql_list.append(genSQL(db, tb, ipv4_col_list, fqdn_col_list, getParam("ioc_table"), True))
print(f"CREATE STREAMING LIVE TABLE {getParam('inc_iochits_table')}\nAS")
print("\nUNION ALL\n".join(sql_list) + ";")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary tables for fast ad hoc historical IOC search
# MAGIC 
# MAGIC * To speed up ad hoc historical IOC search, you can create and maintain summary tables using Delta Live Tables. Since summary tables are created on curated base tables, you would have figured out which columns have IOCs in them and optimized the IOC extraction logic.
# MAGIC * Run the following command to generate the SQL for the DLT pipeline and copy the SQL
# MAGIC * Follow the [DLT quick start guide](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-quickstart.html) and paste the SQL into the SQL notebook for the DLT pipeline

# COMMAND ----------

def genSummaryTableSQL(db, tb, ioc_str):
  full_table_name = db + "." + tb
  stream_name = "stream(" + full_table_name + ")"
  summary_sql_str = f'''
CREATE STREAMING LIVE TABLE ioc_summary_{tb}
AS
SELECT aug.ts_day, aug.obs_value, aug.src_data, aug.src_ip, aug.dst_ip, count(*) AS cnt, min(aug.ts) AS first_seen, max(aug.ts) AS last_seen
FROM
  (
  SELECT '{full_table_name}' AS src_data, extracted_obs AS obs_value, exp.ts::timestamp, date_trunc('DAY', timestamp(exp.ts)) as ts_day, exp.id_orig_h as src_ip, exp.id_resp_h as dst_ip
  FROM
    (
    SELECT d.*,
      {ioc_str}
    FROM {stream_name} AS d
    )  AS exp LATERAL VIEW explode(exp.extracted_obslist) AS extracted_obs
  ) AS aug
GROUP BY ts_day, obs_value, src_data, src_ip, dst_ip;
  '''
  return summary_sql_str

summary_tables = [[getParam("db_name"), "dns", """concat(
        regexp_extract_all(d.query, '(\\\\d+\\\\.\\\\d+\\.\\\\d+\\.\\\\d+)', 0),
        ARRAY(d.id_orig_h),
        ARRAY(d.id_resp_h),
        regexp_extract_all(d.query, '((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\\\.)+[A-Za-z]{2,6}', 0)
        ) AS extracted_obslist
"""], [getParam("db_name"), "http", """concat(
        regexp_extract_all(d.referrer, '(\\\\d+\\\\.\\\\d+\\\\.\\\\d+\\\\.\\\\d+)', 0),
        ARRAY(d.id_orig_h),
        ARRAY(d.host),
        ARRAY(d.id_resp_h),
        regexp_extract_all(d.referrer, '((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\\\.)+[A-Za-z]{2,6}', 0)
        ) AS extracted_obslist
"""]]
sql_list = []
for (db, tb, ioc_str) in summary_tables:
  sql_list.append(genSummaryTableSQL(db, tb, ioc_str))
print("\n".join(sql_list))


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Cleanup

# COMMAND ----------

if env=="test":
  #spark.sql(f"drop schema {getParam('db_name')} CASCADE") 
  pass
elif env=="demo":
  spark.sql(f"truncate table {getParam('iochits_table')}")

# COMMAND ----------


