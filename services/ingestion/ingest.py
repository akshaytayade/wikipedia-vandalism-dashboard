import json
import logging
import time
import requests
import sseclient

LOG_FILE = "/logs/ingest.log"
WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def get_wikimedia_stream_client():
    try:
        response = requests.get(WIKIMEDIA_STREAM_URL, stream=True, timeout=10)
        response.raise_for_status()
        logging.info("Connected to Wikimedia stream")
        client = sseclient.SSEClient(response)
        return client
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to connect to Wikimedia stream: {e}")
        exit(1)

def main():
    logging.info("Starting Wikimedia ingestion...")

    client = get_wikimedia_stream_client()

    for event in client:
        if event.event == 'message':
            try:
                change = json.loads(event.data)
                if change.get("type") == "edit" and change.get("namespace") == 0:
                    change["ingested_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ")
                    logging.info(f"Ingested edit: {change.get('title')}")
            except json.JSONDecodeError:
                logging.warning(f"Invalid JSON: {event.data}")
            except Exception as e:
                logging.error(f"Unexpected error: {e}", exc_info=True)

if __name__ == "__main__":
    main()
