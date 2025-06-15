# End-to-End-Data-Engineering-on-Databricks


This is a comprehensive project that brings together key concepts and tools covered in the **Databricks Data Engineer Associate** certification and real-world data engineering workflows using the **Databricks Lakehouse Platform**.

It demonstrates practical experience in:

- Unity Catalog
- Delta Lake
- Delta Live Tables (DLT)
- Spark Structured Streaming
- Databricks Jobs
- Git integration with Databricks Repos
- ADLS Gen2 integration using External & Managed Locations
- Python & SQL-based expectations and SCD handling

---

##  Project Overview

### 📂 Data Flow
Raw source data (CSV/JSON) from **ADLS Gen2** is ingested into:

- **Bronze** layer using Autoloader and streaming
- **Silver** layer using cleaning rules (DLT expectations)
- **Gold** layer using business logic (aggregations, deduplication, joins)

All pipelines are fully orchestrated using **DLT pipelines** and **Databricks Jobs**.

---

## 🔧 Tools & Technologies

- **Platform**: Azure Databricks (Free Trial)
- **Storage**: ADLS Gen2 via External Volumes
- **Ingestion**: Autoloader (cloudFiles)
- **Processing**: Spark + PySpark
- **Table Format**: Delta Lake
- **Pipelines**: Delta Live Tables (DLT)
- **Governance**: Unity Catalog
- **Automation**: Databricks Jobs
- **Version Control**: Git (via Databricks Repos)

---

## 📌 Key Concepts Practiced

| Topic                      | Covered |
|---------------------------|---------|
| Delta Lake & Transaction Log | ✅     |
| MERGE INTO & SCD Handling     | ✅     |
| Streaming & Micro-Batching    | ✅     |
| Checkpointing & Triggers     | ✅     |
| Z-Ordering & Compaction      | ✅     |
| Vacuum & Data Retention      | ✅     |
| CREATE OR REFRESH STREAMING TABLE | ✅ |
| Unity Catalog: Managed vs External Locations | ✅ |
| External Volumes & ADLS Integration | ✅ |
| Structured Streaming to Delta Tables | ✅ |
| SQL Constraints in DLT       | ✅     |

---

##  How to Reproduce

1. Set up **Unity Catalog**, **External Location**, and **Volumes**
2. Upload datasets to ADLS paths
3. Use the provided notebooks or DLT pipelines to ingest and clean data
4. Use `MERGE INTO`, expectations, and SCD handling for upserts
5. Query Delta tables in SQL or Python

---

##  What I Learned

- How to build a lakehouse architecture using real streaming pipelines
- Hands-on with schema enforcement, versioning, time travel, and ACID guarantees
- How to orchestrate streaming data jobs with DLT and Databricks Jobs
- Unity Catalog’s role in secure, scalable governance

---
- Thanks to Ramesh Ratnasamy for this certification course on udemy!


