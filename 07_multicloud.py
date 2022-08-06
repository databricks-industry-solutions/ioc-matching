# Databricks notebook source
# MAGIC %md
# MAGIC # Multi-cloud/region Federated Queries using JDBC External Tables
# MAGIC 
# MAGIC **Authors**: Zafer Bilaloglu & Lipyeow Lim
# MAGIC 
# MAGIC This notebook demonstrates how to use JDBC external tables and a union-all view to achieve federated query capabilities from a "master" workspace (this one) to a "table" that straddles across multiple workspaces in multiple clouds or regions. This federated query approach is targeted at scenarios where:
# MAGIC 1. it is not possible to consolidate log data to a single workspace because of **data sovereignty** regulations
# MAGIC 2. the **egress cost** to consolidate data from one cloud or region to the central workspace is prohibitive.
# MAGIC 
# MAGIC Note that this notebook illustrates the core concepts and is not intended to be a production setup script. In production, the creation of external table is only done once by the system administrator (and not by the regular user). The regular users perform their analytical queries by interacting with the union-all views or external tables only.
# MAGIC 
# MAGIC ![usecase_image](https://raw.githubusercontent.com/lipyeowlim/public/main/img/multicloud/multicloud-cybersecurity_arch.png)
# MAGIC 
# MAGIC ## Pre-requisites & setup
# MAGIC * The Databricks Simba JDBC driver needs to be installed on the cluster at the master workspace. Note that there are subtle differences between v2.6.22 and v2.6.25.
# MAGIC   * Download it from here: https://databricks.com/spark/jdbc-drivers-download.
# MAGIC   * Install the library in your workspace. See https://docs.databricks.com/libraries/workspace-libraries.html
# MAGIC   * Add the library to your cluster. See https://docs.databricks.com/libraries/cluster-libraries.html
# MAGIC   * You will need an init-script in your cluster's "advanced configuration" tab. The init script will copy the workspace-installed library (dbfs path) to the `/databricks/jars/` path on the cluster nodes. Once the jar is in `/databricks/jars`, it will be included in the Java CLASSPATH. Copy the following python code snipplet into a notebook to create the init-script (replace `PATH_TO_JAR_FILE` with the dbfs path where your jar was installed in the workspace):
# MAGIC 
# MAGIC ```
# MAGIC     dbutils.fs.mkdirs("dbfs:/ioc_matching/scripts/")
# MAGIC     dbutils.fs.put("dbfs:/ioc_matching/scripts/install_jdbc.sh", """
# MAGIC       cp /dbfs/PATH_TO_JAR_FILE /databricks/jars/DatabricksJDBC42.jar
# MAGIC       """, True)
# MAGIC ```
# MAGIC   * You can use the following shell commands in a notebook to check if the jar files are installed on your cluster:
# MAGIC 
# MAGIC ```
# MAGIC     %sh
# MAGIC     
# MAGIC     ls /dbfs/FileStore/jars | grep DatabricksJDBC
# MAGIC     # Note that once the jar file is copied to /databricks/jars/ by the init script, it will be automatically included in the CLASSPATH
# MAGIC     ls /databricks/jars/DatabricksJDBC42.jar
# MAGIC     ls /databricks/jars | grep JDBC
# MAGIC     echo $CLASSPATH | sed 's/:/\n/g' | grep JDBC
# MAGIC ```
# MAGIC 
# MAGIC * You will need a user or service account (aka service principal) on each of the workspaces to be federated. Data sovereignity regulations will determine the level of data access for each of these user/service accounts in each of the workspaces to be federated.
# MAGIC * Run the `02_ioc_matching` notebook to create the required `iochits` table in all the workspaces to be federated
# MAGIC * Create a personal access token (PAT) in each of the workspaces to be federated. See https://docs.databricks.com/dev-tools/api/latest/authentication.html. 
# MAGIC * Store the PAT in Databricks secrets on the master workspace (this one). See https://docs.databricks.com/security/secrets/index.html.
# MAGIC * Copy the JDBC URL for each of the workspaces to be federated. Go to `Compute -> your cluster -> Advanced Options -> JDBC/ODBC -> JDBC URL`. Change the `PWD` option to `REPLACEWITHPAT`. 
# MAGIC * Fill out the configuration json in Cmd 3. For v2.6.25 onwards, 
# MAGIC     * the driver has changed from `com.simba.spark.jdbc.Driver` to `com.databricks.client.jdbc.Driver`
# MAGIC     * the JDBC url has changed from `jdbc:spark://e2-demo-west.cloud.databricks.com:443/default;httpPath=sql/protocolv1/o/2556758628403379/0730-172948-runts698;transportMode=http;ssl=1;AuthMech=3;UID=token;PWD=REPLACEWITHPAT` to `jdbc:databricks://e2-demo-west.cloud.databricks.com:443;httpPath=sql/protocolv1/o/2556758628403379/0730-172948-runts698;transportMode=http;ssl=1;AuthMech=3;UseNativeQuery=0;UID=token;PWD=REPLACEWITHPAT`
# MAGIC 
# MAGIC ## Configuring Access Control for Data Sovereignty
# MAGIC 
# MAGIC * To enforce data sovereignity rules on the federated workspaces
# MAGIC     * Create a separate user or service account in that workspace to be used for federated queries. You can then control the access privileges to tables and views for that user account. See https://docs.databricks.com/security/access-control/table-acls/object-privileges.html.
# MAGIC     * Ensure that the JDBC URL is tied to a HC cluster. Note that the JDBC URL is tied to both the user/service account associated with the PAT and to the cluster. The privileges of the JDBC connection will be tied to the account associated with the PAT.
# MAGIC * To enforce access privileges at the master workspace, you must ensure that users only use a high-concurrency (HC) cluster to query the views and external tables.
# MAGIC * Use views at the source/federated workspaces to control or mask access to columns with protected information. The external tables at the master workspace will then be created against the views.
# MAGIC * There are several places where you have the ability to enforce access privileges:
# MAGIC     * access privileges of the source tables or views at the workspaces to be federated
# MAGIC     * access privileges of the external table at the master workspace
# MAGIC     * access privileges of the union-all view at the master workspace

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,Set Configuration Parameters: list of workspaces the table straddles over
# first two workspaces are in AWS, the 3rd one is in azure
multicloud_cfg={
  "table": getParam("iochits_table"),
  "federated_view": getParam("iochits_table") + "_federated",
  "workspaces": [
    {
      "name": "local"
    },
    {
      "name": "e2_demo_west",
      "jdbc": {
        "url": "jdbc:databricks://e2-demo-west.cloud.databricks.com:443;httpPath=sql/protocolv1/o/2556758628403379/0730-172948-runts698;transportMode=http;ssl=1;AuthMech=3;UseNativeQuery=0;UID=token;PWD=REPLACEWITHPAT",
        "driver": "com.databricks.client.jdbc.Driver",
        "token_scope": "lipyeow-sec01",
        "token_key": "e2-demo-west-pat"
      }
    },
    {
      "name": "azure",
      "jdbc": {
        "url": "jdbc:databricks://eastus2.azuredatabricks.net:443;httpPath=sql/protocolv1/o/5206439413157315/0812-164905-tear862;transportMode=http;ssl=1;AuthMech=3;UseNativeQuery=0;UID=token;PWD=REPLACEWITHPAT",
        "driver": "com.databricks.client.jdbc.Driver",
        "token_scope": "lipyeow-sec01",
        "token_key": "db-azure-pat"
      }
    }
  ]
}

print(json.dumps(multicloud_cfg, indent = 2))

# COMMAND ----------

# DBTITLE 1,Generate & execute DDLs to create external table for the remote JDBC sources
table_list = []
for w in multicloud_cfg["workspaces"]:
  if w["name"] == "local":
    table_list.append(multicloud_cfg["table"])
    continue
  token = dbutils.secrets.get(scope=w["jdbc"]["token_scope"], key=w["jdbc"]["token_key"])
  local_table_name = multicloud_cfg["table"] + "_" + w["name"]
  table_list.append(local_table_name)
  ddlstr = f"""
    DROP TABLE IF EXISTS {local_table_name}
  """
  print(ddlstr)
  spark.sql(ddlstr)
  ddlstr = f"""
    CREATE TABLE {local_table_name}
    USING org.apache.spark.sql.jdbc
    OPTIONS (
      url '{w["jdbc"]["url"].replace("REPLACEWITHPAT",token)}',
      dbtable '{multicloud_cfg["table"]}',
      driver '{w["jdbc"]["driver"]}'
    )
  """
  print(ddlstr)
  spark.sql(ddlstr)

print(table_list)

# COMMAND ----------

# DBTITLE 1,Create the federated union all view
view_name = multicloud_cfg["federated_view"]
drop_view_ddl = f"DROP VIEW IF EXISTS {view_name}\n"
create_view_ddl = f"""CREATE OR REPLACE VIEW {view_name}\nAS\n""" + "\nUNION ALL\n".join(f"SELECT * FROM {t}\n" for t in table_list) 
print(drop_view_ddl)
spark.sql(drop_view_ddl)
print(create_view_ddl)
spark.sql(create_view_ddl)


# COMMAND ----------

# DBTITLE 1,Query the tables sequentially (for debugging & testing)
for t in table_list:
  sqlstr = f"""
    SELECT * FROM {t} where matched_ioc='192.168.202.75'
    """
  print(sqlstr)
  df = spark.sql(sqlstr)
  display(df)


# COMMAND ----------

# DBTITLE 1,Query against federated view (concurrent queries to remote sources)
sqlstr = f"""
SELECT * 
FROM {view_name} 
WHERE matched_ioc='192.168.202.75'
"""

print(sqlstr)
display(spark.sql(sqlstr))


# COMMAND ----------

# DBTITLE 1,Let's check the query plan to see if the filters were pushed down
sqlstr=f"""
EXPLAIN 
SELECT * 
FROM {view_name}
WHERE matched_ioc='192.168.202.75'
"""

print(sqlstr)
display(spark.sql(sqlstr))

# COMMAND ----------

# MAGIC %md
# MAGIC If you look into the explain output, in the `Scan JDBCRelation` operator, we see that the `PushedFilters` include the filter on the IP address. So the filters are pushed into the remote workspaces and only the results will be egressed to the central/local databricks workspace.

# COMMAND ----------


