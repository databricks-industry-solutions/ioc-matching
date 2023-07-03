# Databricks notebook source
# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC select * from list_secrets() where scope= 'lipyeow-sec01';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS ioc_matching_lipyeow_lim.iochits_e2_demo_west;
# MAGIC   
# MAGIC
# MAGIC CREATE TABLE ioc_matching_lipyeow_lim.iochits_e2_demo_west
# MAGIC   USING org.apache.spark.sql.jdbc
# MAGIC   OPTIONS (
# MAGIC     url 'jdbc:databricks://e2-demo-west.cloud.databricks.com:443;httpPath=sql/protocolv1/o/2556758628403379/0730-172948-runts698;transportMode=http;ssl=1;AuthMech=3;UseNativeQuery=0;UID=token;PWD=[REDACTED]',      dbtable 'ioc_matching_lipyeow_lim.iochits',
# MAGIC     driver 'com.databricks.client.jdbc.Driver'
# MAGIC   );
# MAGIC   
# MAGIC DROP TABLE IF EXISTS ioc_matching_lipyeow_lim.iochits_azure;
# MAGIC   
# MAGIC CREATE TABLE ioc_matching_lipyeow_lim.iochits_azure
# MAGIC   USING org.apache.spark.sql.jdbc
# MAGIC   OPTIONS (
# MAGIC     url 'jdbc:databricks://adb-984752964297111.11.azuredatabricks.net:443/default;transportMode=http;ssl=1;UseNativeQuery=0;httpPath=sql/protocolv1/o/984752964297111/0812-165520-brine786;AuthMech=3;UID=token;PWD\[REDACTED]',
# MAGIC     dbtable 'ioc_matching_lipyeow_lim.iochits',
# MAGIC     driver 'com.databricks.client.jdbc.Driver'
# MAGIC   );
# MAGIC   
