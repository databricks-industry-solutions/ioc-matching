# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Load Bigger Data Sets for the summary table tutorial
# MAGIC 
# MAGIC The L7 log data set is extracted from MACCDC pcaps using zeek.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,Download http data
# MAGIC %sh
# MAGIC 
# MAGIC mkdir /dbfs/tmp/maccdc2012
# MAGIC cd /dbfs/tmp/maccdc2012
# MAGIC pwd
# MAGIC echo "Removing all files"
# MAGIC rm -rf *
# MAGIC 
# MAGIC for idx in 00 01 02 03 04 05 06 07 08 09 10 11 12 14 15 16;
# MAGIC do
# MAGIC   mkdir $idx
# MAGIC   cd $idx
# MAGIC   pwd 
# MAGIC   for fname in http.log.gz dns.log.gz;
# MAGIC   do
# MAGIC     dlpath="https://raw.githubusercontent.com/lipyeow-lim/security-datasets01/main/maccdc-2012/$idx/$fname"
# MAGIC     wget $dlpath
# MAGIC     gzip -d $fname
# MAGIC   done
# MAGIC   cd ..
# MAGIC done
# MAGIC 
# MAGIC ls -lR

# COMMAND ----------

# DBTITLE 1,Load dns & http data
from pyspark.sql.functions import col
from pyspark.sql.types import *
download_path="/tmp/maccdc2012"
tables=["http", "dns"]
folders=["00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "14", "15", "16"]
# Load the zeek logs extracted from pcaps
for t in tables:
  tb = f"{getParam('db_name')}.{t}_big"
  for f in folders:
    jsonfile=f"{download_path}/{f}/{t}.log"
    print(f"Loading {jsonfile} into {tb} ...")
    df = spark.read.format("json").load(jsonfile).withColumn("eventDate", col("ts").cast("Timestamp").cast("Date"))
    df.write.option("mergeSchema", "true").partitionBy("eventDate").mode("append").saveAsTable(tb)


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 'http' as table_name, count(*), min(eventDate), max(eventDate)
# MAGIC from ioc_matching_lipyeow_lim.http_big
# MAGIC union all
# MAGIC select 'dns' as table_name, count(*), min(eventDate), max(eventDate)
# MAGIC from ioc_matching_lipyeow_lim.dns_big

# COMMAND ----------

# MAGIC %sql
# MAGIC --truncate table ioc_matching_lipyeow_lim.http_big;
