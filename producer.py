import json
from datetime import datetime
import time
from kafka import KafkaProducer
import requests

API_KEY = "007128d8b5mshbbd6e4780eae04cp1c5b50jsnb3ec739ab739"
url = "https://rapidapi.com/apiheya/api/sky-scrapper/api/v1/flights/searchFlights?originSkyId=LOND&destinationSkyId=NYCA&originEntityId=27544008&destinationEntityId=27537542&date=2024-07-01&adults=1&currency=USD&market=en-US&countryCode=US"
producer = KafkaProducer(bootstrap_servers="localhost:9092")

headers = {
    "X-RapidAPI-Key": API_KEY,
    "X-RapidAPI-Host": "sky-scrapper.p.rapidapi.com"
}

while True:
    try:
        response = requests.get(url, headers=headers)
        if response.headers.get("Content-Type") == "application/json":
            data = response.json()
            print("Response:", data)  # Print the response to debug

            # Assuming Elasticsearch is running locally on default port
            es_url = "http://localhost:9200/flights_new1/_doc"
            es_headers = {"Content-Type": "application/json"}

            # Send data to Elasticsearch
            es_response = requests.post(es_url, headers=es_headers, data=json.dumps(data))
            print("Elasticsearch Response:", es_response.text)

            print(f"{datetime.now()} Produced 1 record")
        else:
            print("Received non-JSON response. Skipping...")
    except Exception as e:
        print(f"Error processing response: {e}")
        print(f"Response content: {response.content if 'response' in locals() else None}")

    time.sleep(1)
