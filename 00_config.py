# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/ioc-matching. 

# COMMAND ----------

# MAGIC %md
# MAGIC *If you are looking for the entry-point notebook, please go to `02_ioc_matching.py` instead.*
# MAGIC 
# MAGIC ## Initialize All Configuration Parameters
# MAGIC 
# MAGIC * Creates the cfg json object to store configuration parameters
# MAGIC * Defines the getParam function for accessing the parameters
# MAGIC * This notebook to to be shared/run by any notebook that requires access to the configuration parameters

# COMMAND ----------

import os
import re
import json

if "env" not in vars():
  env = "test"

if "cfg" not in vars():  
  cfg={}
  cfg["useremail"] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
  cfg["username"] = cfg["useremail"].split('@')[0]
  cfg["username_sql_compatible"] = re.sub('\W', '_', cfg["username"])
  cfg["db_name"] = f"ioc_matching_{cfg['username_sql_compatible']}"
  cfg["data_path"] = f"/tmp/ioc_matching/data/{cfg['useremail']}/"
  cfg["iochits_table"] = cfg["db_name"] + ".iochits"
  cfg["inc_iochits_table"] = cfg["db_name"] + ".inc_iochits"
  cfg["ioc_table"] = cfg["db_name"] + ".ioc"
  cfg["included_databases"] = [ cfg["db_name"] ] if env == "test" else [ cfg["db_name"], 'vpc_flow_logs']
  cfg["excluded_tables"] = [cfg["iochits_table"], cfg["ioc_table"], cfg["inc_iochits_table"]]

if "getParam" not in vars():
  def getParam(param):
    assert param in cfg
    return cfg[param]

print(f"env = {env}")
print(json.dumps(cfg, indent=2))

# COMMAND ----------


