# Glintt_Next_Assessment_Case

End-to-end batch analytics pipeline implemented with **Apache Spark and Delta Lake**, following a **Raw–Silver–Gold data lake architecture**.  
The solution models **Slowly Changing Dimensions (SCD Type 2)** and **fact tables**, and is aligned with an **AWS-oriented architecture** for scalable analytics workloads.

---

## 📌 Problem Overview

The goal of this challenge is to design and implement a batch analytics platform capable of:
- Ingesting incremental raw data
- Applying data quality and transformation rules
- Managing historical dimensions
- Producing analytics-ready datasets
- Following enterprise-grade data engineering best practices

The implementation avoids managed SaaS analytics platforms and focuses on open, portable technologies.

---

## 🏗️ Architecture Overview

The proposed architecture is based on an **AWS data lake pattern**, implemented locally using Spark and Delta Lake.

📁 **Raw Zone (Bronze)**  
- Immutable CSV batch files  
- Incremental ingestion  
- Source-of-truth data  

📁 **Silver Zone (Curated)**  
- Cleaned and conformed datasets  
- Snapshot-level grain  
- Stored as **Delta Lake tables**

📁 **Gold Zone (Serving)**  
- Analytics-ready dimensional model  
- **SCD Type 2 dimensions**
- Fact tables optimized for BI and SQL analytics

### Architecture Diagram
📌 ![Architecture Diagram](diagrams/architecture.png)

---

## ⭐ Dimensional Model (Star Schema)

The Gold layer follows a **star schema** design:

### Dimensions
- **dim_athlete** (SCD Type 2)
- **dim_games** (SCD Type 0 – immutable reference data)

### Fact Table
- **fact_olympic_results**

📌 ![Star Schema](diagrams/start_schema.png)

---

## 📊 Data Model Details

### 🧍 Athlete Dimension (SCD Type 2)
Tracks historical changes in athlete attributes:
- height
- weight

Each change generates a new version with:
- `effective_from`
- `effective_to`
- `is_current`

### 🎮 Games Dimension (SCD Type 0)
Reference data for Olympic games:
- year
- season
- city

No historical changes are expected.

### 🧮 Results Fact Table
Stores event-level results and references both dimensions using surrogate keys.

---

## 🔄 Pipeline Flow

1. **Raw → Silver**
   - Read latest CSV batch
   - Apply schema and data types
   - Deduplicate records
   - Write Delta snapshot

2. **Silver → Gold (Dimensions)**
   - Apply SCD logic using Delta MERGE
   - Preserve full history
   - Enforce one current record per business key

3. **Silver → Gold (Fact)**
   - Resolve surrogate keys
   - Insert new fact records
   - Ensure referential integrity

---

## 🛠️ Technology Stack

- **Apache Spark 3.5**
- **Delta Lake**
- **PySpark**
- **SQL (MERGE-based SCD logic)**
- **Mypy** for static type checking
- **AWS-oriented architecture design**

---

## 📂 Folder Description

- **data/raw_samples/**  
  Sample CSV files used to simulate incremental batch ingestion for athletes, games, and Olympic results.

- **diagrams/**  
  Architecture and star schema diagrams used to explain the overall platform design and data model.

- **src/**  
  Spark + Delta Lake batch pipelines:
  - `pipeline_athlete.py`: Athlete dimension with SCD Type 2 logic.
  - `pipeline_games.py`: Games dimension (static/SCD Type 0).
  - `pipeline_fact.py`: Olympic results fact table.
  - `config.py`: Shared configuration and paths.

- **requirements.txt**  
  Python dependencies required to run the project.

- **mypy.ini**  
  Static type checking configuration to ensure code quality.
