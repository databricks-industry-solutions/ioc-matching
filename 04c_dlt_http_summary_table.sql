-- Databricks notebook source
create streaming live table summary_http
as
select 
    date_trunc('HOUR', f.ts::timestamp) as ts_bkt, 
    f.obs_value,
    f.`id.orig_h`,
    f.`id.resp_h`,
    to_json(struct(f.method, f.status_code, f.user_agent)) as info, 
    count(*) as cnt,
    min(f.ts::timestamp) as firstseen,
    max(f.ts::timestamp) as lastseen
from 
  (
  select 
    e.*, 
    explode(e.extracted_obslist) AS obs_value
  from 
    ( 
    select *, 
      CONCAT(
          ARRAY(d.`id.orig_h`),
          ARRAY(d.`id.resp_h`),
          regexp_extract_all(case when d.referrer is null then '' else d.referrer end, '(\\d+\\.\\d+\\.\\d+\\.\\d+)', 0)
      ) AS extracted_obslist 
    from stream(ioc_matching_lipyeow_lim.http_big) as d
    ) as e
  ) as f
group by ts_bkt, f.obs_value, f.`id.orig_h`, f.`id.resp_h`, f.method, f.status_code, f.user_agent

-- COMMAND ----------


