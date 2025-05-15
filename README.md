# 🌦️ Scalable Real-Time Weather Data Processing Pipeline

This project implements a **scalable, real-time weather data processing pipeline** using **Azure Event Hub**, **Databricks Delta Lake**, and **Apache Spark Structured Streaming**. The pipeline ingests simulated weather data, performs real-time cleansing and transformation, and generates **heatwave alerts** based on defined temperature and humidity thresholds. It also supports **Slowly Changing Dimension Type 2 (SCD Type 2)** to maintain the historical state of alerts.

---

## 🔧 Architecture Overview


![ChatGPT Image May 15, 2025, 09_50_38 PM](https://github.com/user-attachments/assets/c6dd81ea-a8ee-4aed-a43e-9264d80bcf46)



### **Data Source**

* A local **Weather API** built using [OpenWeatherMap API](https://openweathermap.org/api) simulates real-time weather data generation.

### **Data Ingestion**

* Simulated weather data is streamed to **Azure Event Hub**.

### **Data Processing in Databricks**

* **Bronze Layer (`weather_bronze`)**: Ingests raw weather data as-is.
* **Silver Layer (`weather_silver`)**: Performs data cleansing and transformation.
* **Gold Layer (`heatwave_alerts_gold`)**: Generates alerts for heatwave conditions and implements **SCD Type 2** logic for historical tracking.

---

## ✨ Key Features

* Real-time streaming ingestion using **Azure Event Hub** and **Spark Structured Streaming**
* Robust data cleansing and transformation logic in the **silver layer**
* Business rule implementation to generate heatwave alerts:

  * **Temperature > 35°C**
  * **Humidity < 30%**
* **SCD Type 2** support to track historical changes in alert status with `is_active` flags and `end_date` columns
* Checkpointing and fault tolerance to prevent data loss or duplication
* Efficient upserts and merges using **Delta Lake**

---

## 📊 Components Breakdown

### 1. **Data Ingestion**

* The local Weather API (powered by [OpenWeatherMap](https://openweathermap.org/api)) pushes data to **Azure Event Hub**.
* **Databricks Structured Streaming** reads from Event Hub and writes raw data into the **bronze layer**.

### 2. **Data Cleansing (Silver Layer)**

* Invalid or incomplete records are filtered out.
* Timestamps and other fields are normalized.
* Clean data is stored in `weather_silver`.

### 3. **Heatwave Alerts (Gold Layer)**

* Heatwave conditions are identified:

  * Temperature > 35°C
  * Humidity < 30%
* Alerts are generated with **SCD Type 2** implementation:

  * Active alerts: `is_active = 'yes'`
  * Expired alerts: `is_active = 'no'` and `end_date` set
* Upserts and merges are performed into `heatwave_alerts_gold` using Delta Lake.

---

## 📚 Technologies Used

* **Apache Spark Structured Streaming**
* **Delta Lake** (for ACID transactions and efficient upserts)
* **Azure Event Hub** (for real-time ingestion)
* **Databricks Platform**
* **Python & PySpark**

---

## ▶️ How to Run

1. Start the local Weather API simulator (using [OpenWeatherMap API](https://openweathermap.org/api)).
2. Configure Azure Event Hub and update connection details in Databricks.
3. Run the streaming ingestion notebook to populate the **bronze layer**.
4. Run the **silver layer** transformation notebook.
5. Run the **gold layer** notebook to generate heatwave alerts with SCD Type 2 logic.

---

## ✨ Future Enhancements

* Add more complex alert logic (e.g., **cold wave**, **storm warnings**)
* Integrate **machine learning** models for predictive alerts
* Build **dashboard visualizations** for alert monitoring and analytics

---

## ✉️ Contact

For any queries or suggestions, feel free to reach out! abhavya155@gmail.com

---

> ⚡ Built to demonstrate scalable and resilient data pipelines using Azure and Databricks.
