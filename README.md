# Hospital-Patient-Admissions-
Hospital Patient Admissions – Azure Synapse Data Engineering Project

This project implements a complete end-to-end data engineering pipeline using Azure Synapse Analytics to process 2023–2024 hospital admission data across six hospitals. The solution follows the Medallion Architecture (Bronze → Silver → Gold) using ADLS Gen2, Synapse Pipelines, Spark (PySpark), Delta Lake, and Power BI.

🔷 Key Features

GitHub → ADLS ingestion using Synapse Pipelines (Copy Activity).
Bronze Layer: Raw ingestion, external tables, views, and calculated columns.
Silver Layer: Data cleaning, standardization, enrichment, and Delta Lake storage.
Gold Layer: Complete Star Schema (fact + dimensions) for analytics.

Power BI dashboards for insights (revenue, admissions trends, department performance).
Serverless SQL external tables for BI and unified querying.
Secure architecture using Managed Identity & ADLS access control.

🛠️ Azure Services Used

Azure Data Lake Gen2 · Azure Synapse Pipelines · Spark Notebooks · Serverless SQL · Delta Lake · Power BI · Managed Identity
