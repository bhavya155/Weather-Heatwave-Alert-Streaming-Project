**🔧 Azure Event Hub Setup for Real-Time Weather Data Streaming

✅ Step 1: Create an Azure Event Hubs Namespace

Go to Azure Portal

Search for Event Hubs in the top search bar.

Click “+ Add” to create a new Event Hubs Namespace:

Name: weatherstream-namespace

Pricing Tier: Standard

Location: Choose your region

Throughput Units: 1 (default is fine for small test data)

Click Review + Create → Create

✅ Step 2: Create an Event Hub Inside the Namespace

Once your namespace is created, open it.

Under Entities, click + Event Hub.

Set:

Name: weatherstream

Partition Count: 2 (default)

Message Retention: 1 day

Click Create

✅ Step 3: Create a Shared Access Policy (SAS Policy)

In your Event Hub namespace → Shared access policies

Click + Add

Name it: WeatherStreamPolicy

Permissions:

Tick both Send and Listen

Click Create

Copy the Connection String – Primary Key. You'll need this in Databricks.

✅ Step 4: Send Events (from local machine or script)

Use a Python script to push data to Event Hub: [refer](https://github.com/bhavya155/Weather-Heatwave-Alert-Streaming-Project/blob/ee281281cd6c04dacd4567a79db9eacc8b81ab89/wheatherforDiffferentCities.py)


**🔧 DataBricks

✅ Step 1: Read Event Hub Data in Databricks


