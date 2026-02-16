# 🏨 HappyBooking: End-to-End Modern Data Stack Project

This project implements a comprehensive **Data Engineering** solution for a hotel chain, processing reservation data through both real-time streaming and batch ingestion, ensuring data quality, and delivering actionable insights via interactive dashboards.



## 🛠️ Tech Stack
* **Platform:** Microsoft Fabric (Lakehouse & Data Factory)
* **Data Processing:** PySpark (Apache Spark)
* **Data Ingestion:** Local CSV (Batch) & Dockerized Python API (Streaming)
* **Storage:** Delta Lake (Medallion Architecture: Bronze, Silver, Gold)
* **Visualization:** Power BI (Direct Lake Mode)
* **DevOps:** GitHub Integration & CI/CD (GitHub Actions)

## 🏗️ Data Architecture (Medallion Layers)

1. **Bronze (Raw):** Stores raw data from diverse sources directly into Delta tables without any modifications.
2. **Silver (Cleaned):** Implements data quality gates. Removes duplicates, normalizes data types (dates, prices), and filters out anomalies using logic similar to `Great Expectations`.
3. **Gold (Curated):** Business-ready aggregated tables such as "Hotel Performance Analytics" and "Cancellation Trends."

## 🔄 Automation & Data Quality (CI/CD)
* **Fabric Pipeline:** Orchestrates all notebooks into a seamless workflow with automated triggers.
* **GitHub Actions:** A CI/CD pipeline triggers on every push/pull request to perform syntax checks and data validation.
* **Data Quality Guardrails:** Detects and filters out negative prices, null IDs, and logical date errors before they reach the Silver layer.

## 📊 Business Insights
* **Revenue Analytics:** Tracking total revenue per hotel and average daily rates.
* **Operational Efficiency:** Analysis of booking lead times and cancellation ratios to optimize occupancy.

---

## 🚀 How to Run
1. Spin up the Docker containers to start the streaming API.
2. Trigger the `HappyBooking_Main_Pipeline` in Microsoft Fabric to process the data.
3. Open the Power BI report to visualize the live processed data.

---
*Developed as a showcase of modern cloud data engineering practices.*
