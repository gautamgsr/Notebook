# Real-Time Transaction Pattern Detection Pipeline

## 🧠 Project Overview

This project is designed to process **10,000+ transactions per second** in real-time using a distributed data pipeline. The raw transaction data is ingested from **AWS S3**, processed using **Databricks with PySpark**, and written back to S3 in a refined format.

The main goal of the pipeline is to **detect transaction patterns** and **maintain synchronized state** across multiple lookup tables to keep producer and consumer data consistent.

---

## 🧰 Tools & Technologies Used

- **Databricks** – For distributed data processing and orchestration
- **Python & PySpark** – Data transformations, filtering, aggregations
- **AWS S3** – Source and sink for raw and processed data
- **PostgreSQL** – Lookup tables and stateful information storage
- **Delta Lake** – (Optional, if used) for ACID transactions and upserts

---

## 🔄 Data Flow

1. **Ingestion**:
   - Raw transaction data is continuously picked from AWS S3.
   - Format: JSON / CSV / Parquet (based on your setup)

2. **Processing (on Databricks)**:
   - Applied transformation logic using PySpark
   - Used **lookup tables** from PostgreSQL to enrich and validate records
   - Applied **pattern detection logic** (e.g., frequency, outliers, duplicates)

3. **State Management**:
   - Used **upsert (merge) operations** to keep track of evolving states.
   - Ensured **producer-consumer table synchronization** using lookup mapping logic.

4. **Output**:
   - Final transformed data written back to a structured location in **S3**.

---

## 📊 Features

- High-throughput processing: 10K+ transactions/second
- Real-time enrichment using lookup tables
- Smart upserts to manage evolving data states
- Robust and scalable architecture
- Modular and easy-to-extend PySpark code

---

## ⚙️ Setup Instructions

1. **Clone the Repository**
   ```bash
   git clone https://github.com/your-username/transaction-pattern-detector.git
   cd transaction-pattern-detector
