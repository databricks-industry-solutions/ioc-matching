# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Historical IOC Search
# MAGIC 
# MAGIC Whenever I hear about a new threat on the news or threat intel feeds, I want to grab the associated IoCs and check if my organization is affected. Sometimes the threat activity can happen months before news of the threat is released to the public.
# MAGIC 
# MAGIC Searching through years of _raw_ DNS or HTTP data can be slow. Standard trick is to use summary tables/indexes - build a smaller highly-aggregated table to narrow the search before hitting the raw data.

# COMMAND ----------

# DBTITLE 1,Total number of rows of http data
# MAGIC %sql
# MAGIC 
# MAGIC select count(*) as total_raw_rows
# MAGIC from ioc_matching_lipyeow_lim.http_big

# COMMAND ----------

# DBTITLE 1,Peek at the http data
# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC from ioc_matching_lipyeow_lim.http_big

# COMMAND ----------

# DBTITLE 1,Design & test out the summarizing scheme (group by attributes)
# MAGIC %sql
# MAGIC 
<<<<<<< HEAD
# MAGIC select count(*) as num_rows
# MAGIC from (
=======
# MAGIC --select count(*) as num_rows
# MAGIC --from (
>>>>>>> 0c2a525 (fix regex and add  summary table tutorial)
# MAGIC 
# MAGIC   select date_trunc('HOUR', ts::timestamp) as ts_bkt, 
# MAGIC     `id.orig_h`,
# MAGIC     `id.resp_h`,
# MAGIC     method,
# MAGIC     status_code,
# MAGIC     user_agent, 
# MAGIC     count(*) as cnt,
# MAGIC     min(ts::timestamp) as firstseen,
# MAGIC     max(ts::timestamp) as lastseen
# MAGIC   from ioc_matching_lipyeow_lim.http_big
# MAGIC   group by ts_bkt, `id.orig_h`, `id.resp_h`, method, status_code, user_agent
# MAGIC 
<<<<<<< HEAD
# MAGIC )
=======
# MAGIC --)
>>>>>>> 0c2a525 (fix regex and add  summary table tutorial)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Add the observables extraction to make a summary index structure
# MAGIC 
# MAGIC In production, we would
# MAGIC 1. exclude any private IPv4s in the summary tables to make the summary tables even more compact
# MAGIC 1. exclude some popular IPv4s and FQDNs that are known to be benign
# MAGIC 1. extract not just IPv4, but also FQDN, hashes etc.

# COMMAND ----------

# DBTITLE 1,Add the ipv4 extraction and turn this into an indexing structure
# MAGIC %sql
# MAGIC --select count(*)
# MAGIC --from (
# MAGIC select 
# MAGIC     date_trunc('HOUR', f.ts::timestamp) as ts_bkt, 
# MAGIC     f.obs_value,
# MAGIC     f.`id.orig_h`,
# MAGIC     f.`id.resp_h`,
# MAGIC     to_json(struct(f.method, f.status_code, f.user_agent)) as info, 
# MAGIC     count(*) as cnt,
# MAGIC     min(f.ts::timestamp) as firstseen,
# MAGIC     max(f.ts::timestamp) as lastseen
# MAGIC from 
# MAGIC   ( -- this subquery uses the pattern from episode 1
# MAGIC   select 
# MAGIC     e.*, 
# MAGIC     explode(e.extracted_obslist) AS obs_value
# MAGIC   from 
# MAGIC     ( 
# MAGIC     select *, 
# MAGIC       concat(
# MAGIC           ARRAY(d.`id.orig_h`),
# MAGIC           ARRAY(d.`id.resp_h`), 
# MAGIC           regexp_extract_all(case when d.referrer is null then '' else d.referrer end, '(\\d+\\.\\d+\\.\\d+\\.\\d+)', 0)
# MAGIC       ) as extracted_obslist 
# MAGIC     from ioc_matching_lipyeow_lim.http_big as d
# MAGIC     ) as e
# MAGIC   ) as f
# MAGIC group by ts_bkt, f.obs_value, f.`id.orig_h`, f.`id.resp_h`, f.method, f.status_code, f.user_agent
# MAGIC --)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## How big should your time buckets be?
# MAGIC 
# MAGIC It's a trade-off between cost and analyst convenience.
# MAGIC 
# MAGIC ## Create a DLT job for the summarization
# MAGIC 
# MAGIC 1. Create a SQL notebook using the above query
# MAGIC 1. Create a DLT job
# MAGIC 1. Run the DLT job (and schedule to run periodically)
# MAGIC 
# MAGIC ## Let's see how to use the summary tables
# MAGIC 
# MAGIC 1. Create a view to unions the summary tables for all the different log types 
# MAGIC 1. Aggregate the summary table entries in the view definition
# MAGIC 1. Query the view based on observable value and/or a time filter (and/or any of the group by attributes)

# COMMAND ----------

# DBTITLE 1,Create view
# MAGIC %sql
# MAGIC 
# MAGIC drop view if exists ioc_matching_lipyeow_lim.summary_all;
# MAGIC create view ioc_matching_lipyeow_lim.summary_all
# MAGIC as
# MAGIC select 
# MAGIC   ts_bkt,
# MAGIC   obs_value, 
# MAGIC   `id.orig_h`, 
# MAGIC   `id.resp_h`, 
# MAGIC   'http' as log_type,
# MAGIC   sum(cnt) as cnt, 
# MAGIC   min(firstseen) as firstseen, 
# MAGIC   max(lastseen) as lastseen,
# MAGIC   array_agg(info) as details
# MAGIC from ioc_matching_lipyeow_lim.summary_http
# MAGIC group by ts_bkt, obs_value, `id.orig_h`, `id.resp_h`
# MAGIC --union all
# MAGIC -- other log types like dns, ssh, kerberos etc.
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Query the view to look for evil
# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC from ioc_matching_lipyeow_lim.summary_all
<<<<<<< HEAD
# MAGIC where ts_bkt between '2012-03-16T00:00:00.000+0000' and '2012-03-17T00:00:00.000+0000' 
# MAGIC   and obs_value = '192.168.21.253'
# MAGIC order by cnt
=======
# MAGIC where obs_value = '192.168.21.253'
>>>>>>> 0c2a525 (fix regex and add  summary table tutorial)

# COMMAND ----------

# DBTITLE 1,Use the results from the view to zoom in on the raw or bronze data
# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC from ioc_matching_lipyeow_lim.http_big as d
<<<<<<< HEAD
# MAGIC where d.ts::timestamp between '2012-03-16T19:35:15.660+0000' and '2012-03-16T19:35:25.410+0000'
# MAGIC   and `id.orig_h` = '192.168.202.88' and `id.resp_h` = '192.168.21.253'
=======
# MAGIC where d.ts::timestamp between '2012-03-16T19:39:45.300+0000' and '2012-03-16T19:43:18.080+0000'
# MAGIC   and `id.orig_h` = '192.168.202.4' and `id.resp_h` = '192.168.21.253'
>>>>>>> 0c2a525 (fix regex and add  summary table tutorial)

# COMMAND ----------


