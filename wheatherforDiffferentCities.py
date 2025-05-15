import requests
import json
import time
from azure.eventhub import EventHubProducerClient, EventData

# -------- CONFIGURATION -------- #
openweather_api_key = "b3a72e45eb0a7f261537c53b5effa88e"
interval_seconds = 300  # Run every 5 minutes

cities = [
    {"name": "Bangalore", "country": "IN"},
    {"name": "Chennai", "country": "IN"},
    {"name": "Mangalore", "country": "IN"},
    {"name": "Hyderabad", "country": "IN"},
    {"name": "Mumbai", "country": "IN"},
    {"name": "Delhi", "country": "IN"}
]

# Event Hub connection details
eventhub_connection_string = (
    "Endpoint=sb://ehnamespacewheather.servicebus.windows.net/;"
    "SharedAccessKeyName=databricks;"
    "SharedAccessKey=SNlBC58bCdkq/URWW9aClZAmY2vgSuVBC+AEhC9v3ZE=;"
    "EntityPath=wheather"
)
eventhub_name = "wheather"

# -------- FUNCTIONS -------- #

def get_weather_data(city, country):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city},{country}&appid={openweather_api_key}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Failed for {city}: {response.status_code} - {response.text}")
        return None

def send_to_eventhub(data, producer):
    try:
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(json.dumps(data)))
        producer.send_batch(event_data_batch)
        print(f"✅ Sent full JSON data for {data['name']} to Event Hub.")
    except Exception as e:
        print(f"❌ Failed to send data to Event Hub: {e}")

# -------- MAIN LOOP -------- #

def main():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=eventhub_connection_string,
        eventhub_name=eventhub_name
    )

    print("🚀 Weather streaming started for multiple cities. Press Ctrl+C to stop.")
    try:
        while True:
            for city_info in cities:
                city_name = city_info["name"]
                country_code = city_info["country"]
                weather_data = get_weather_data(city_name, country_code)
                if weather_data:
                    send_to_eventhub(weather_data, producer)
                time.sleep(1)  # Avoid rapid-fire API calls
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("🛑 Stopping streaming...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
