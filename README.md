# Ad Hoc IOC Matching on All Tables

version 1.1 (2022-07-01)

![usecase_image](https://raw.githubusercontent.com/lipyeowlim/public/main/img/ioc-matching/ir-ioc-matching.png)

## Use cases covered by the notebooks in this solution accelerator

* **Schema-agnostic IOC matching scan**: During an incident response (IR) engagement, an analyst might want to perform an ad hoc scan of all the data (logs, telemetry, etc.) in a security lakehouse for a given list of atomic Indicators-of-Compromise (IOCs) without the need to have deep understanding of the table schemas. The `02_ioc_matching` notebook addresses this use case.
* **Continuous IOC matching**: The approach in the `02_ioc_matching` notebook can be easily adapted to perform incremental or continuous IOC matching using [Delta Live Tables (DLT)](https://docs.databricks.com/data-engineering/delta-live-tables/index.html). An example is given in the `03_dlt_ioc_matching` notebook.
* **Ad hoc historical IOC search**: Historical IOC search at interactive speeds can be done using summary tables constructed using DLT. An example is given in the `04_dlt_summary_table` notebook. The `06_verify_dlt` notebook provides a series of steps to verify the DLT capabilities.
* **Multi-cloud/region federated query**: Log ingestion and IOC matching can happen in each cloud or region without incurring egress costs. Hunting and triage of IOC hits can use federated queries from a single workspace to get results back from the workspaces in each cloud or region. The `07_multicloud` notebook demonstrates the use of multi-cloud and multi-region federated queries. 

## Overview of this Notebook
* Setup: Initialize configuration parameters, create database and load three delta tables: `dns`, `http`, `ioc`
* **Step 1**: Scan all table schemas/metadata in the Databricks workspace
* **Step 2**: Infer fields that might contain IOC/observables (eg. IPv4, IPv6, fqdn, MD5, SHA1, SHA256)
* **Step 3**: Run join queries for those fields against a table of known bad IOCs. The resulting matches are stored in the `iochits` table to be fed into a downstream SIEM/SOAR and/or triaged directly by analysts.
* Generate SQL DLT pipeline for continuous/incremental IOC matching pipeline
* Generate SQL DLT pipeline for maintaining summary tables for fast historical IOC search

## Value

* Simplicity: agility & operational effectiveness
* Schema-agnostic: variety of data sources during IR
* Performance: Large historical time window

## Running this notebook

* If you received a DBC file for this solution accelerator, you just need to import the DBC file
* If you received a zip file for this solution accelerator, the easiest way to get started is to:
  * Create a git repo
  * Unzip and copy the code into the git repo
  * Add the repo in the Databricks console -> Repos -> "Add Repo"
  * Once the repo is added, you can go to the repo and click on the `02_ioc_matching` notebook.
* Note that only the dns & http sample data are included in the notebook. The `vpc_flow_logs` database is not included in the notebook.
