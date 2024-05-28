<img src="https://github.com/lipyeowlim/public/raw/main/img/logo/databricks_cyber_logo_v1.png" width="600px">

[![DBR](https://img.shields.io/badge/DBR-10.4ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/10.4ml.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

# Indicator-of-Compromise (IOC) Matching
___
Contact Author: <cybersecurity@databricks.com>


## Use Cases 

* **Schema-agnostic IOC matching scan**: During an incident response (IR) engagement, an analyst or incident responder might want to perform an ad hoc scan of all the data (logs, telemetry, etc.) in a security lakehouse for a given list of atomic Indicators-of-Compromise (IOCs) without the need to have deep understanding of the table schemas. The `02_ioc_matching` notebook addresses this use case.
* **Continuous IOC matching**: The approach in the `02_ioc_matching` notebook can be easily adapted to perform incremental or continuous IOC matching using [Delta Live Tables (DLT)](https://docs.databricks.com/data-engineering/delta-live-tables/index.html). An example is given in the `03_dlt_ioc_matching` notebook.
* **Ad hoc historical IOC search**: Historical IOC search at interactive speeds can be done using summary tables constructed using DLT. An example is given in the `04_dlt_summary_table` notebook. The `06_verify_dlt` notebook provides a series of steps to verify the DLT capabilities.
* **Multi-cloud/region federated query**: Log ingestion and IOC matching can happen in each cloud or region without incurring egress costs. Hunting and triage of IOC hits can use federated queries from a single workspace to get results back from the workspaces in each cloud or region. The `07_multicloud` notebook demonstrates the use of multi-cloud and multi-region federated queries. 
* **Fully-automated continuous IOC matching with continuous IOC updates**: The streaming IOC matching approach in the `03_dlt_ioc_matching` notebook and the summary table approach in the `04_dlt_summary_table` notebook can be combined and extended to fully automate the IOC matching process even when the curated set of IOCs are constantly updated. In particular, when a new IOC is added, not only should newly ingested log data be matched against the new IOC, but the historical data needs to be matched against the new IOC. The `08_handling_ioc_updates` notebook demonstrates these concepts.

## Running this solution accelerator

* The main entry point is the `02_ioc_matching.py` notebook.
* The entry point for the multi-cloud use case is the `07_multicloud.py` notebook.
* The entry point for the continuous IOC matching with continuous IOC updates use case is `08_handling_ioc_updates.sql` notebook.
