# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/ioc-matching. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert new data in order to verify DLT pipelines
# MAGIC 
# MAGIC * Insert two new DNS tuples with unix timestamp set to 2022-04-18T00:00:00

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

insert_sql = f"""
INSERT INTO {getParam("db_name")}.dns VALUES 
('1650240000','CEDU9bbR8k7laNPbe','10.10.117.209','54709','192.168.207.4','53','udp','4162','-','liveupdate.symantecliveupdate.com','1','C_INTERNET','1','A','-','-','F','F','T','F','0','-','-','F'),
('1650240000','COJ3ng1AmGcOef3xSb','192.168.202.75','51760','192.168.207.4','53','udp','14805','-','teredo.ipv6.microsoft.com','1','C_INTERNET','1','A','-','-','F','F','T','F','0','-','-','F');
"""

print(insert_sql)
spark.sql(insert_sql)

# COMMAND ----------


