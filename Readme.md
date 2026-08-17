# 🌦️ Weather Data Integration & AI Platform

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-orange?logo=apachespark)
![Databricks](https://img.shields.io/badge/Databricks-Data%20Engineering-red?logo=databricks)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Lakehouse-blue)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE?logo=apacheairflow)
![AI/ML](https://img.shields.io/badge/AI%2FML-Enabled-purple)
![GitHub](https://img.shields.io/badge/GitHub-Version%20Control-black?logo=github)

## 📌 Overview

`weather-data-integration` is an end-to-end **Data Engineering, Lakehouse, AI/ML, and orchestration platform** built to ingest, process, analyze, and enrich weather data using modern data technologies.

The project started as a simple weather API integration and evolved through six phases into a complete data platform combining:

* External API integration
* Python
* PySpark
* Databricks
* Delta Lake
* Medallion Architecture
* Data quality
* Feature engineering
* Deterministic analytics
* AI/ML processing
* LLM-generated insights
* Apache Airflow orchestration
* Automated pipeline execution
* Dependency management between data processing stages

The main goal is to demonstrate how a traditional API integration solution can evolve into a **production-oriented Data Engineering + AI platform**.

---

# 🏗️ Architecture

The final architecture combines **data ingestion, Lakehouse processing, AI/ML, and workflow orchestration**.

```text
                         ┌──────────────────────┐
                         │      Open-Meteo      │
                         │      Weather API     │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │       Python        │
                         │    API Ingestion    │
                         └──────────┬───────────┘
                                    │
                                    ▼
                    ┌──────────────────────────────┐
                    │        BRONZE LAYER          │
                    │                              │
                    │ Raw weather observations     │
                    │ Delta Lake                   │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │        SILVER LAYER          │
                    │                              │
                    │ Cleaned / normalized data    │
                    │ Data validation              │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │         GOLD LAYER            │
                    │                              │
                    │ Business analytics            │
                    │ Aggregations                  │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │       AI FEATURES             │
                    │                              │
                    │ Feature engineering           │
                    │ Weather indicators            │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │                             │
                    ▼                             ▼
          ┌──────────────────┐          ┌──────────────────┐
          │  Deterministic   │          │      AI/ML       │
          │     Analysis     │          │     Analysis     │
          └────────┬─────────┘          └────────┬─────────┘
                   │                             │
                   └──────────────┬──────────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │       LLM        │
                         │     Insights     │
                         └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │   AI INSIGHTS    │
                         │ Business-ready   │
                         │      data        │
                         └──────────────────┘


                 ┌──────────────────────────────┐
                 │        Apache Airflow        │
                 │         Orchestration        │
                 └──────────────┬───────────────┘
                                │
                                ▼
                       Pipeline Scheduling
                                │
                                ▼
                       Databricks Jobs
                                │
                                ▼
                       Data + AI Pipeline
```

Airflow is responsible for **orchestration**, while Databricks remains responsible for the distributed data processing workloads.

---

# 🚀 Project Evolution

The implementation is divided into six phases.

| Phase   | Main Objective                        | Technologies               |
| ------- | ------------------------------------- | -------------------------- |
| Phase 1 | API integration and initial ingestion | Python, Open-Meteo         |
| Phase 2 | Data Engineering foundation           | PySpark, Databricks        |
| Phase 3 | Lakehouse and Medallion Architecture  | Delta Lake, Spark          |
| Phase 4 | Analytics and feature engineering     | PySpark, SQL               |
| Phase 5 | AI/ML and intelligent insights        | AI/ML, LLM                 |
| Phase 6 | Pipeline orchestration                | Apache Airflow, Databricks |

---

# 1️⃣ Phase 1 — Weather API Integration

The first phase established the foundation of the project.

## Objectives

* Connect to the Open-Meteo API
* Retrieve weather information
* Handle API responses
* Normalize JSON responses
* Build the initial ingestion process
* Create reusable Python components

## Data

The ingestion process works with information such as:

* City
* Latitude
* Longitude
* Timestamp
* Temperature
* Humidity
* Wind speed
* Weather conditions
* Precipitation
* Pressure
* Other meteorological indicators

## Initial flow

```text
Open-Meteo API
      │
      ▼
Python HTTP Client
      │
      ▼
JSON Response
      │
      ▼
Data Normalization
      │
      ▼
Structured Weather Data
```

---

# 2️⃣ Phase 2 — PySpark & Databricks

Phase 2 introduced distributed data processing using **Apache Spark** and Databricks.

## Objectives

* Use PySpark for transformations
* Create Spark DataFrames
* Perform scalable processing
* Establish Databricks as the execution environment
* Prepare the project for Lakehouse storage

## Technologies

* Python
* PySpark
* Databricks
* Spark SQL

Example:

```python
from pyspark.sql import functions as F

df = spark.read.json(input_path)

result = (
    df
    .withColumn("temperature_celsius", F.col("temperature"))
    .withColumn("processed_at", F.current_timestamp())
)
```

---

# 3️⃣ Phase 3 — Delta Lake & Medallion Architecture

Phase 3 transformed the project into a Lakehouse architecture.

The analytical storage layer was moved toward **Delta Lake**, providing transactional storage directly integrated with Databricks and Spark.

## Medallion Architecture

### 🥉 Bronze

Raw weather information.

```text
weather_bronze
```

Characteristics:

* Raw API data
* Minimal transformation
* Original ingestion context
* Traceability

### 🥈 Silver

Cleaned and standardized data.

```text
weather_silver
```

Operations include:

* Data type conversion
* Null handling
* Schema standardization
* Timestamp normalization
* Data validation
* Duplicate handling

### 🥇 Gold

Business-oriented analytical data.

```text
weather_gold
```

Operations include:

* Aggregations
* Weather indicators
* Daily statistics
* Risk metrics
* Analytical dimensions

---

# 4️⃣ Phase 4 — Analytics & Feature Engineering

Phase 4 introduced advanced analytics and prepared the platform for AI.

## Objectives

* Generate analytical features
* Create weather indicators
* Detect weather patterns
* Prepare ML-ready datasets
* Implement deterministic analysis
* Establish an AI feature layer

## Example features

```text
temperature_avg
temperature_max
temperature_min
humidity_avg
wind_speed_avg
precipitation_total
temperature_variation
humidity_variation
wind_risk
precipitation_risk
temperature_risk
weather_risk_score
```

## Feature engineering flow

```text
Silver Data
     │
     ▼
Aggregations
     │
     ▼
Derived Metrics
     │
     ▼
Weather Features
     │
     ▼
AI Feature Dataset
```

---

# 5️⃣ Phase 5 — AI/ML & Intelligent Weather Insights

Phase 5 introduced the AI layer.

The objective was to move beyond traditional analytics and generate **intelligent interpretations of weather data**.

## AI architecture

```text
              Gold Weather Data
                      │
                      ▼
              AI Feature Layer
                      │
             ┌────────┴────────┐
             │                 │
             ▼                 ▼
     Deterministic       AI/ML Analysis
       Analysis                 │
             │                  ▼
             │             Risk Scoring
             │                  │
             └────────┬─────────┘
                      │
                      ▼
                LLM Analysis
                      │
                      ▼
                 AI Insights
```

## AI components

The Phase 5 implementation contains dedicated processing components such as:

```text
01_ai_features.py
02_ai_weather_analysis.py
03_ai_insights.py
04_ai_quality_check.py
```

These components provide separation between:

* Feature generation
* Weather analysis
* Insight generation
* AI/data-quality validation

---

# 🤖 Deterministic Analysis

The platform does not rely exclusively on AI.

A deterministic analysis layer provides:

* Explainable rules
* Reproducible results
* Consistent risk classification
* Validation of AI-generated results
* Baseline analysis

Example:

```text
IF temperature > threshold
    → HIGH_TEMPERATURE_RISK

IF wind_speed > threshold
    → HIGH_WIND_RISK

IF precipitation > threshold
    → HIGH_PRECIPITATION_RISK
```

This establishes an important architectural principle:

> **AI should augment reliable data engineering and analytical rules, not replace them.**

---

# 🧠 LLM-Generated Insights

The LLM layer converts structured analysis into human-readable insights.

Example input:

```text
Temperature: 36°C
Humidity: 25%
Wind Speed: 42 km/h
Precipitation: 0 mm

Risk: HIGH
```

Possible output:

```text
The current weather conditions indicate elevated heat and
wind-related risk. High temperatures combined with low humidity
may increase heat stress, while strong winds may create
additional operational risks.
```

The generated insight can be stored together with metadata such as:

```text
ai_model
prompt_version
analysis_timestamp
ai_status
```

This improves traceability and reproducibility.

---

# 6️⃣ Phase 6 — Apache Airflow Orchestration

Phase 6 introduces **Apache Airflow** as the orchestration layer.

The objective is to move from manually executing individual Databricks notebooks/scripts to a coordinated and schedulable data pipeline.

## Objectives

* Schedule the weather pipeline
* Define task dependencies
* Trigger Databricks processing
* Monitor pipeline execution
* Handle failures
* Enable retries
* Centralize workflow management
* Separate orchestration from data processing

---

# 🔄 Phase 6 Pipeline

The Airflow DAG coordinates the complete processing flow.

```text
                    ┌──────────────────┐
                    │    Airflow DAG   │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │  Start Pipeline  │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Weather Ingestion│
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Bronze Processing│
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Silver Processing│
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │  Gold Processing │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │  AI Features     │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │  AI Analysis     │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │  AI Insights     │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Quality Check    │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Pipeline Success │
                    └──────────────────┘
```

---

# 🌬️ Airflow + Databricks Architecture

A key architectural decision in Phase 6 is separating **orchestration** from **processing**.

```text
┌─────────────────────────────────────┐
│             Apache Airflow          │
│                                     │
│  Scheduling                         │
│  Dependencies                       │
│  Retries                            │
│  Monitoring                         │
│  Workflow Management                │
└──────────────────┬──────────────────┘
                   │
                   │ Trigger
                   ▼
┌─────────────────────────────────────┐
│             Databricks              │
│                                     │
│  Spark Processing                   │
│  Delta Lake                         │
│  Data Transformations               │
│  AI/ML Processing                   │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│             Delta Lake              │
│                                     │
│  Bronze                             │
│  Silver                             │
│  Gold                               │
│  AI Features                        │
│  AI Analysis                        │
│  AI Insights                        │
└─────────────────────────────────────┘
```

Airflow therefore acts as the **control plane**, while Databricks acts as the **data processing platform**.

---

# 📅 Scheduling

The pipeline can be configured to execute periodically.

Example:

```text
Daily
  │
  ▼
Airflow Scheduler
  │
  ▼
Weather Pipeline
  │
  ▼
Databricks
```

A cron expression can be used depending on the desired execution frequency.

Example:

```text
0 1 * * *
```

This represents a daily execution at 01:00.

---

# 🔁 Task Dependencies

Airflow ensures that downstream tasks execute only after their dependencies have successfully completed.

Example:

```text
ingest_weather
       │
       ▼
run_bronze
       │
       ▼
run_silver
       │
       ▼
run_gold
       │
       ▼
run_ai_features
       │
       ▼
run_ai_analysis
       │
       ▼
run_ai_insights
       │
       ▼
quality_check
```

This makes the pipeline easier to operate and troubleshoot.

---

# ♻️ Retry & Failure Handling

Airflow provides workflow-level reliability mechanisms.

Example:

```text
Task Failed
     │
     ▼
Retry
     │
     ├── Success → Continue
     │
     └── Failure → Mark Task Failed
```

This is particularly useful for:

* API failures
* Temporary Databricks errors
* Network problems
* Authentication issues
* Transient infrastructure failures

---

# 🔐 Databricks Connection

Airflow communicates with Databricks through an Airflow connection.

The connection can be configured using the connection ID:

```text
databricks_default
```

The architecture intentionally keeps authentication information outside the DAG code.

Conceptually:

```text
Airflow DAG
     │
     ▼
databricks_default
     │
     ▼
Databricks API
     │
     ▼
Databricks Job
```

Secrets and tokens should be managed using an appropriate secret-management mechanism rather than hardcoded in source code.

---

# 🗂️ Repository Structure

The final repository can be organized as follows:

```text
weather-data-integration/
│
├── README.md
├── requirements.txt
├── databricks.yml
├── .gitignore
│
├── config/
│   ├── config.py
│   └── environments/
│
├── src/
│   ├── ingestion/
│   │   └── open_meteo.py
│   │
│   ├── bronze/
│   │   └── weather_bronze.py
│   │
│   ├── silver/
│   │   └── weather_silver.py
│   │
│   ├── gold/
│   │   └── weather_gold.py
│   │
│   ├── features/
│   │   └── ai_features.py
│   │
│   ├── analysis/
│   │   ├── deterministic_analysis.py
│   │   └── ai_analysis.py
│   │
│   └── insights/
│       └── ai_insights.py
│
├── notebooks/
│   ├── 01_ingestion
│   ├── 02_bronze
│   ├── 03_silver
│   ├── 04_gold
│   ├── 05_ai_features
│   ├── 06_ai_analysis
│   └── 07_ai_insights
│
├── airflow/
│   ├── dags/
│   │   └── weather_pipeline.py
│   │
│   ├── Dockerfile
│   └── requirements.txt
│
├── tests/
│   ├── test_ingestion.py
│   ├── test_transformations.py
│   ├── test_features.py
│   └── test_ai_analysis.py
│
└── docs/
    ├── architecture.md
    ├── data_model.md
    ├── ai_architecture.md
    └── airflow.md
```

---

# 🐳 Local Airflow Environment

Phase 6 can be executed locally using Docker.

Example architecture:

```text
┌──────────────────────────────┐
│       Docker Compose         │
│                              │
│  ┌────────────────────────┐  │
│  │   Airflow Container    │  │
│  │                        │  │
│  │   Scheduler            │  │
│  │   API Server           │  │
│  │   DAGs                 │  │
│  └───────────┬────────────┘  │
│              │               │
└──────────────┼───────────────┘
               │
               │ API
               ▼
       ┌─────────────────┐
       │    Databricks   │
       │                 │
       │ Spark / Delta   │
       └─────────────────┘
```

The Airflow environment is isolated from the Databricks execution environment.

---

# 📊 Data Model

The logical data flow is:

```text
weather_bronze
       │
       ▼
weather_silver
       │
       ▼
weather_gold
       │
       ▼
weather_ai_features
       │
       ▼
weather_ai_analysis
       │
       ▼
weather_ai_insights
```

Each dataset has a clearly defined responsibility.

---

# 🔄 Complete End-to-End Pipeline

The final project can be represented as:

```text
                  ┌────────────────────┐
                  │    Open-Meteo API  │
                  └─────────┬──────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │      Airflow       │
                  │   Orchestration    │
                  └─────────┬──────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │ Python Ingestion   │
                  └─────────┬──────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │       Bronze       │
                  │      Delta Lake    │
                  └─────────┬──────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │       Silver       │
                  │  Clean / Validate  │
                  └─────────┬──────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │        Gold        │
                  │     Analytics      │
                  └─────────┬──────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │    AI Features     │
                  └─────────┬──────────┘
                            │
                   ┌────────┴────────┐
                   ▼                 ▼
            ┌─────────────┐   ┌─────────────┐
            │Deterministic│   │    AI/ML    │
            │   Analysis  │   │   Analysis  │
            └──────┬──────┘   └──────┬──────┘
                   │                 │
                   └────────┬────────┘
                            ▼
                     ┌────────────┐
                     │    LLM     │
                     └─────┬──────┘
                           │
                           ▼
                  ┌────────────────────┐
                  │    AI Insights     │
                  └─────────┬──────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │   Quality Check    │
                  └────────────────────┘
```

---

# 🧱 Data Engineering Principles

## Idempotency

The pipeline should be safely re-runnable without generating unintended duplicates.

```text
Same Input
    +
Same Processing
    ↓
Consistent Final State
```

This is important for:

* Retries
* Backfills
* Failed executions
* Reprocessing
* Scheduled pipelines

---

## Data Quality

The pipeline validates:

* Required fields
* Data types
* Null values
* Duplicate records
* Timestamp validity
* Geographic information
* Weather measurements
* AI output structure

---

## Schema Management

Each layer maintains a defined schema.

```text
Bronze
  ↓
Raw schema

Silver
  ↓
Standardized schema

Gold
  ↓
Analytical schema

AI Features
  ↓
ML/AI schema

AI Insights
  ↓
Business-facing schema
```

---

# 🧪 Testing

Testing covers both application logic and data transformations.

Recommended testing layers:

```text
Unit Tests
     ↓
Transformation Tests
     ↓
Data Quality Tests
     ↓
Integration Tests
     ↓
Pipeline Tests
     ↓
AI Output Validation
```

Examples:

* API response validation
* Schema validation
* Transformation validation
* Feature calculation tests
* Risk classification tests
* AI output validation
* Airflow DAG validation

---

# 📈 Observability

A production-oriented implementation should monitor:

* DAG execution status
* Task execution status
* Records ingested
* Records transformed
* Records rejected
* Processing duration
* API failures
* Databricks failures
* Data quality failures
* AI processing failures
* LLM latency
* LLM failures

Example:

```text
Airflow
   │
   ├── DAG Status
   ├── Task Status
   ├── Retry Count
   ├── Execution Duration
   │
   └── Pipeline Logs
            │
            ▼
       Databricks
            │
            ├── Spark Logs
            ├── Data Quality
            └── Processing Metrics
```

---

# 🔐 Security

The project follows basic security principles:

* No credentials in source code
* No API tokens committed to Git
* Environment-specific configuration
* Secret management
* Least-privilege access
* Separate development and production configurations

Sensitive credentials should be injected through environment variables, Airflow connections, Databricks secrets, or an appropriate external secret manager.

---

# 📦 Technology Stack

| Technology     | Purpose                                |
| -------------- | -------------------------------------- |
| Python         | API integration and application logic  |
| Open-Meteo     | Weather data source                    |
| Apache Spark   | Distributed processing                 |
| PySpark        | Data transformation                    |
| Databricks     | Cloud Data Engineering platform        |
| Delta Lake     | Lakehouse storage                      |
| Spark SQL      | Analytical processing                  |
| Apache Airflow | Workflow orchestration                 |
| AI/ML          | Predictive and analytical capabilities |
| LLM            | Natural-language insights              |
| Docker         | Local Airflow environment              |
| Git            | Version control                        |
| GitHub         | Source code management                 |

---

# 🔁 Reliability

The combination of Airflow and Databricks provides a clear separation of responsibilities.

### Airflow

Responsible for:

* Scheduling
* Dependencies
* Retries
* Monitoring
* Workflow management

### Databricks

Responsible for:

* Spark processing
* Delta Lake
* Data transformations
* Feature engineering
* AI/ML processing

This separation makes the platform easier to operate and evolve.

---

# 📈 Scalability

Although the current weather dataset is relatively small, the architecture is designed to scale.

Possible future data sources include:

* Thousands of cities
* Multiple weather providers
* Historical weather datasets
* IoT weather sensors
* Satellite data
* Streaming weather events
* Large-scale forecasting datasets

The same architecture can support substantially larger workloads by leveraging Spark and Databricks.

---

# 🔮 Future Improvements

## Orchestration

* Airflow Sensors
* TaskGroups
* Dynamic task mapping
* External task dependencies
* SLA monitoring
* Alerting
* Production metadata database

## Databricks

* Databricks Workflows
* Unity Catalog
* Databricks Asset Bundles
* MLflow
* Model Registry
* Automated deployment

## Data Engineering

* Incremental processing
* Structured Streaming
* Data contracts
* Automated schema evolution
* Advanced data quality framework
* Data lineage

## AI/ML

* Weather forecasting
* Anomaly detection
* Predictive risk modeling
* ML model monitoring
* Automated model retraining

## Generative AI

* RAG
* Weather knowledge base
* AI agents
* Natural-language data exploration
* Multi-agent weather analysis
* Automated operational alerts

## DevOps

* GitHub Actions
* Automated CI/CD
* Infrastructure as Code
* Automated testing
* DEV → QA → PROD promotion

---

# 🎯 Project Goals

The complete project demonstrates practical knowledge across four major areas.

## Data Engineering

* API ingestion
* ETL/ELT
* PySpark
* Delta Lake
* Medallion Architecture
* Data quality
* Data modeling
* Distributed processing

## Cloud & Databricks

* Databricks
* Lakehouse architecture
* Serverless processing
* Delta tables
* Spark SQL
* Data pipelines

## AI/ML

* Feature engineering
* Deterministic analytics
* Risk classification
* AI/ML integration
* LLM integration
* Prompt versioning
* AI-generated insights

## Orchestration & DevOps

* Apache Airflow
* DAG design
* Scheduling
* Task dependencies
* Retries
* Monitoring
* Docker
* Git
* CI/CD concepts

---

# 🏆 Final Architecture

After completing Phases 1–6, the project represents an end-to-end modern data platform:

```text
┌─────────────────────────────────────────────────────────┐
│                     DATA SOURCE                          │
│                                                         │
│                    Open-Meteo API                       │
└───────────────────────────┬─────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                     APACHE AIRFLOW                      │
│                                                         │
│ Scheduling • Dependencies • Retries • Monitoring        │
└───────────────────────────┬─────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                      DATABRICKS                         │
│                                                         │
│                   Apache Spark / PySpark                │
└───────────────────────────┬─────────────────────────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │       BRONZE        │
                 │      Delta Lake     │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │       SILVER        │
                 │  Clean / Validated  │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │        GOLD         │
                 │      Analytics      │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │    AI FEATURES      │
                 │ Feature Engineering │
                 └──────────┬──────────┘
                            │
                  ┌─────────┴─────────┐
                  │                   │
                  ▼                   ▼
          ┌───────────────┐   ┌───────────────┐
          │ Deterministic │   │    AI / ML    │
          │    Analysis   │   │    Analysis   │
          └───────┬───────┘   └───────┬───────┘
                  │                   │
                  └─────────┬─────────┘
                            │
                            ▼
                    ┌──────────────┐
                    │     LLM      │
                    │    Insights  │
                    └──────┬───────┘
                           │
                           ▼
                 ┌─────────────────────┐
                 │     AI INSIGHTS      │
                 │ Business-ready data  │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │   QUALITY CHECK     │
                 └─────────────────────┘
```

---

# 📊 Project Status

```text
Phase 1  ✅ API Integration
Phase 2  ✅ PySpark / Databricks
Phase 3  ✅ Delta Lake / Medallion Architecture
Phase 4  ✅ Analytics / Feature Engineering
Phase 5  ✅ AI/ML / LLM Insights
Phase 6  ✅ Apache Airflow Orchestration

Overall Status: 🚀 Phase 6 Completed
```

---

# 👨‍💻 Author

**Ruben Cruz**

Data Engineer | Integration Specialist | AI & Data Engineering

Areas of interest:

* Data Engineering
* AI/ML
* Generative AI
* Cloud Architecture
* Data Integration
* APIs
* Lakehouse Architecture
* Databricks
* Apache Airflow
* Enterprise Integration

---

# 📄 License

This project is intended for educational, portfolio, experimentation, and demonstration purposes.

Add an appropriate open-source license before using the project commercially.
