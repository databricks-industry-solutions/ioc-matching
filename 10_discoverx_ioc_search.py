# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # IoC Matching using DiscoverX
# MAGIC
# MAGIC DiscoverX enables you to perform schema-agnostic IoC search - You do not need to know the table schemas in order to find IoCs in your log data.
# MAGIC
# MAGIC
# MAGIC ### Requirements for running this notebook
# MAGIC
# MAGIC * This notebook requires a compute cluster with Unity Catalog support
# MAGIC * This notebook is dependent on the `00_config` notebook to setup the config json
# MAGIC * This notebook requires the dbl-discoverx python library
# MAGIC
# MAGIC ### Running this notebook
# MAGIC
# MAGIC * Click the "Run all" button.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install "dbl-discoverx>=0.0.3"

# COMMAND ----------

dbutils.library.restartPython() 

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,Tweaks in cfg to support UC
cfg["catalog"] = f"{cfg['username_sql_compatible']}_cyber"
cfg["db"] = cfg["db_name"]
cfg["download_path"] = "/tmp/forensics2021"
cfg["folders"] = ["2021-10"]
cfg["tables"] = [
    "conn",
    "dce_rpc",
    "dhcp",
    "dns",
    "dpd",
    "files",
    "http",
    "kerberos",
    "ntlm",
    "ocsp",
    "pe",
    "smb_files",
    "smb_mapping",
    "smtp",
    "ssl",
    "weird",
    "x509"
  ]



# COMMAND ----------

# DBTITLE 1,Create catalog & database
ddls = [
f"""CREATE CATALOG IF NOT EXISTS {getParam('catalog')}""",
f"""USE CATALOG {getParam('catalog')}""",
f"""DROP SCHEMA IF EXISTS {getParam('db')} CASCADE""",
f"""CREATE SCHEMA IF NOT EXISTS {getParam('db')}""" ,
f"""USE {getParam('db')}""" 
]

for d in ddls:
  print(d)
  spark.sql(d)

# COMMAND ----------

# DBTITLE 1,Download zeek data from github repo
# MAGIC %sh
# MAGIC
# MAGIC mkdir -p /dbfs/tmp/forensics2021
# MAGIC cd /dbfs/tmp/forensics2021
# MAGIC pwd
# MAGIC echo "Removing all files"
# MAGIC rm -rf *
# MAGIC echo
# MAGIC wget https://raw.githubusercontent.com/lipyeow-lim/security-datasets01/main/forensics-2021/logs.zip
# MAGIC
# MAGIC unzip logs.zip
# MAGIC
# MAGIC ls -lR

# COMMAND ----------

# DBTITLE 1,Load zeek data set in original data schema
from pyspark.sql.functions import col
from pyspark.sql.types import *

# Load the zeek logs extracted from pcaps
for t in getParam('tables'):
  tb = f"{getParam('db')}.{t}"
  for f in getParam('folders'):
    jsonfile=f"{getParam('download_path')}/{f}/{t}.log"
    print(f"Loading {jsonfile} into {tb} ...")
    df = spark.read.format("json").load(jsonfile).withColumn("eventDate", col("ts").cast("Timestamp").cast("Date"))
    df.write.option("mergeSchema", "true").partitionBy("eventDate").mode("append").saveAsTable(tb)


# COMMAND ----------

# DBTITLE 1,Let's look at the IP address statistics
sql_str = f"""
select `id.resp_h`, count(*) as cnt 
from {getParam('catalog')}.{getParam('db')}.dns 
group by `id.resp_h` 
order by cnt
"""
display(spark.sql(sql_str))

# COMMAND ----------

# DBTITLE 1,Let's find a fqdn to do the search 
sql_str = f"""
select query, count(*) as cnt 
from {getParam('catalog')}.{getParam('db')}.dns 
group by query 
order by cnt
"""
display(spark.sql(sql_str))

# COMMAND ----------

# DBTITLE 1,Use DiscoverX to scan the data
from discoverx import DX

dx = DX(locale="US")
dx.scan(from_tables=f"{getParam('catalog')}.{getParam('db')}.*", sample_size=100)

# COMMAND ----------

# DBTITLE 1,Let's look at the rules used to discover the columns
dx.display_rules()

# COMMAND ----------

# DBTITLE 1,What exactly does the scan result look like in a dataframe?
display(dx.scan_result)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Schema-agnostic IOC Search

# COMMAND ----------

# DBTITLE 1,Here is how to do schema-agnostic IOC search!
df = dx.search("51.158.108.203", from_tables="*.*.*")
display(df)

# COMMAND ----------

# DBTITLE 1,Let's try searching for a fqdn
df = dx.search("znkbmknmqyvbxi", from_tables="*.*.*", by_class="fqdn")
display(df)

# COMMAND ----------

# DBTITLE 1,Let's specify a minimum score on the fqdn classification
df = dx.search("znkbmknmqyvbxi", from_tables="*.*.*", by_class="fqdn", min_score=1)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Saving the DiscoverX object for a later session

# COMMAND ----------

# DBTITLE 1,Let's save the `dx` metadata to a table for later use
dx.save(full_table_name=f"{getParam('catalog')}.{getParam('db')}.iocdx")

# COMMAND ----------

# DBTITLE 1,Checking the saved metadata
# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from iocdx

# COMMAND ----------

# DBTITLE 1,Load the metadata from the saved table into `dx_new`
dx_new = DX(locale="US")
dx_new.load(full_table_name=f"{getParam('catalog')}.{getParam('db')}.iocdx")

# COMMAND ----------

# DBTITLE 1,Sanity check `dx_new` using a search
df = dx_new.search("znkbmknmqyvbxi", from_tables="*.*.*", by_class="fqdn")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## What does this mean?
# MAGIC
# MAGIC * In the absence of any full text search technology, a security analyst can perform IOC searches without knowing the table schemas in a notebook environment.
# MAGIC * From a cost and TCO perspective, IOC search using DiscoverX mitigates the need for
# MAGIC   * ELT pipelines to normalize the log data into a common data model, and 
# MAGIC   * full text search indexing technology
# MAGIC * Trade cost for search performance and interactive experience 

# COMMAND ----------


