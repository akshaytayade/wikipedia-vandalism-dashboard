# /services/ingestion/ingest.py
import json
import sseclient
import requests
from kafka import KafkaProducer
from shared.logging import setup_logging
import logging

# Configuration
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'raw_edits'
WIKIMEDIA_STREAM_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'
setup_logging() # Sets up the logging module defined in /shared/logging

def get_wikimedia_stream_client():
    """Returns a client for the Wikimedia SSE stream."""
    # The sseclient library handles the request internally.
    # We pass the URL string directly to it.
    # We can also pass requests-compatible arguments like headers or timeout.
    try:
        client = sseclient.SSEClient(WIKIMEDIA_STREAM_URL, timeout=5)
        print(f"Successfully connected to Wikimedia stream at {WIKIMEDIA_STREAM_URL}")
        return client
    except requests.exceptions.RequestException as e:
        # The exception will be raised by the SSEClient constructor if it can't connect.
        print(f"Error connecting to Wikimedia stream: {e}")
        print("Please check the URL and your internet connection.")
        exit(1) # Exit if the connection fails

    return sseclient.SSEClient(response)

def create_kafka_producer():
    """Creates and returns a Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        api_version=(0, 10, 1), # Specify API version for broader compatibility
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def main():
    """
    Connects to the Wikimedia stream and pushes events to Kafka.
    """
    producer = create_kafka_producer()
    client = get_wikimedia_stream_client()

    logging.info("Starting ingestion service...")
    # The SSEClient object is the iterator itself.
    for event in client:
        if event.event == 'message':
            try:
                change = json.loads(event.data)
                # We only care about actual edits to the main namespace
                if change.get('type') == 'edit' and change.get('namespace') == 0:
                    print(f"Publishing edit for page: {change.get('title')}")
                    producer.send(KAFKA_TOPIC, change)
            except json.JSONDecodeError:
                # This can happen if the stream sends a non-JSON message (e.g., a comment or empty line)
                pass # It's safe to ignore these in this case
            except Exception as e:
                print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
