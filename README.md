# 🚓 LAPD Crime Data Analysis Project

This project provides a complete end-to-end data pipeline and analytical workflow for processing, cleaning, normalizing, and visualizing LAPD crime data using SQL Server, Power BI, and a modern data warehouse approach.

---

## 📦 Project Structure

| Layer | Description |
|-------|-------------|
| **Bronze** | Raw data ingested via bulk insert (.txt import) |
| **Silver** | Cleaned and normalized tables following 3NF and snowflake schema principles |
| **Gold** | Business-ready views following star schema principles |
| **Power BI** | Visual dashboard with trends, spatial insights, and time-based patterns |

---

## 🗂️ Data Sources

- **Dataset**: Official LAPD Crime Data  
  [https://catalog.data.gov/dataset/crime-data-from-2020-to-present](https://catalog.data.gov/dataset/crime-data-from-2020-to-present)

- **File Types**:
  - .txt (pre-processed data: the raw CSV produces errors upon bulk insertion)
  - Normalized SQL tables
  - Denormalized, Business-Ready Views

---

## 🧠 Key Features

- ✅ Full ETL pipeline with developed **SQL Server stored procedures**
- ✅ Use of `STRING_SPLIT`, `CASE`, and surrogate keys
- ✅ Data normalization into **dimension and fact tables**
- ✅ **Power BI dashboard** with:
  - Crime trends by time of day, location, and victim demographics
  - Crime status breakdowns and high-risk areas
  - Top crime premises and reporting districts

---

## 📊 Power BI Dashboard Preview

![Power BI Dashboard](/docs/3 - analysis/dashboard-preview.png)  
*Visualizing crime heatmaps, top categories, time trends, and victim profiles*

---

## 🧱 Schema Design

### ❄️ Snowflake Schema
- **Fact Table**: `silver.norm_fact_specifics` (each row = 1 crime incident/report)
- **Dimensions**:
  - `silver.dim_time`
  - `silver.dim_status`
  - `silver.dim_mo_code`
  - `silver.dim_location` (+ sub-dimensions)
  - `silver.dim_method` (+ sub-dimensions)
  - `silver.dim_victim_profile` (+ sub-dimensions)

### ⭐ Star Schema
- **Fact Table**: `gold.fact_crime_specifics` (each row = 1 crime incident/report)
- **Dimensions**:
  - `gold.dim-method`
  - `gold.dim_victim_profile`
  - `gold.dim_mo_code`
  - `gold.dim_location`

---

## 🚀 How to Use

1. Clone the repo
2. Run the scripts in the `/scripts` folder to create tables, load data, and publish views.
3. Run the queries under `/docs/3 - analysis/scripts` to explore data in tabular views.
4. Open the Power BI Dashboard in `/docs/3 - analysis/` to explore visualizations.
5. Optional: extend schema with more demographic and geographic dimensions.

--- 

## 📁 Folder Structure

lapd-crime-data-analysis
├── data_sources/       # Data Sources
├── docs/               # Documentations (diagrams and Power BI dashboard)
├── scripts/            # Scripts for DDL and Loading Data
├── LICENSE             # MIT License
└── README.md           # Project documentation

---

📌 Future Improvements

- Integrate with Azure Data Factory for cloud-based ETL
- Deploy Power BI dashboard via app workspace
- Add machine learning model to predict high-risk areas

--- 

📄 License

This project is licensed under the MIT License.
Data is publicly sourced from the City of Los Angeles Open Data Portal.

