**🔧 Azure Event Hub Setup for Real-Time Weather Data Streaming**

---

### ✅ Step 1: Create an Azure Event Hubs Namespace

* Go to Azure Portal
* Search for **Event Hubs** in the top search bar.
* Click “+ Add” to create a new Event Hubs Namespace:

  * **Name**: `weatherstream-namespace`
  * **Pricing Tier**: premium
  * **Location**: Choose your region
  * **Throughput Units**: 1 (default is fine for small test data)
* Click **Review + Create** → **Create**

### ✅ Step 2: Create an Event Hub Inside the Namespace

* Open your created namespace.
* Under **Entities**, click **+ Event Hub**.
* Set:

  * **Name**: `weatherstream`
  * **Partition Count**: 2 (default)
  * **Message Retention**: 1 day
* Click **Create**

### ✅ Step 3: Create a Shared Access Policy (SAS Policy)

* In your Event Hub namespace, go to **Shared access policies**
* Click **+ Add**
* Name it: `WeatherStreamPolicy`
* Permissions:

  * Tick both **Send** and **Listen**
* Click **Create**
* **Copy the Connection String – Primary Key** for use in Databricks.

### ✅ Step 4: Send Events (from local machine or script)

* Use a Python script to push data to Event Hub:
  [weatherforDiffferentCities.py](https://github.com/bhavya155/Weather-Heatwave-Alert-Streaming-Project/blob/ee281281cd6c04dacd4567a79db9eacc8b81ab89/wheatherforDiffferentCities.py)

---





**🔧 Azure Databricks Setup for Real-Time Weather Data Streaming**

---

### ✅ Step 1: Mount ADLS Gen2 to Databricks (Checkpoint Location)

* Use `dbutils.fs.mount()` with the correct OAuth configs to mount ADLS Gen2.

  [refer](https://github.com/bhavya155/Weather-Heatwave-Alert-Streaming-Project/blob/e4ed3008024c6b6f505b7c5e4a9b9c99dcbb0076/MountingCheckPoint.py)

### ✅ Step 2: Bronze Layer - Load Raw JSON Data from Azure Event Hub

* Use Spark Structured Streaming to ingest from Event Hub and write to Delta format in Bronze layer.

   [refer](https://github.com/bhavya155/Weather-Heatwave-Alert-Streaming-Project/blob/e4ed3008024c6b6f505b7c5e4a9b9c99dcbb0076/StreamingWheather%20Bronze.py)

### ✅ Step 3: Silver Layer - Cleanse & Drop Duplicates

* Read Bronze data and apply deduplication logic based .
* Write cleansed data to Silver.

  [refer](https://github.com/bhavya155/Weather-Heatwave-Alert-Streaming-Project/blob/e4ed3008024c6b6f505b7c5e4a9b9c99dcbb0076/Streaming%20Wheather%20Silver.py)

### ✅ Step 4: Gold Layer - SCD2 for Heatwave Alerts

* Filter records with `temperature > 35` and `humidity < 30`.
* Use `MERGE INTO` logic to implement SCD Type 2 in Delta Lake.

   [refer](https://github.com/bhavya155/Weather-Heatwave-Alert-Streaming-Project/blob/e4ed3008024c6b6f505b7c5e4a9b9c99dcbb0076/StreamingWheatherGold.py)


![image](https://github.com/user-attachments/assets/991620de-0eda-4b01-82cc-2c1d62d9d003)


### ✅ Final Architecture Summary

      ```
      Event Hub
         │
         ▼
      [Bronze Layer]
      Raw JSON (append only)
      
         ▼
      [Silver Layer]
      Cleaned, deduplicated
      
         ▼
      [Gold Layer]
      Heatwave Alerts (SCD2 Type)
      ```
