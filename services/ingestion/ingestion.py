import json
import sseclient
import requests
from kafka import KafkaProducer
from datetime import datetime
import logging
from elasticsearch import Elasticsearch

# Configuration
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'raw_edits'
WIKIMEDIA_STREAM_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'
ES_HOST = 'http://localhost:9200'
ES_LOG_INDEX = 'wikimedia-logs'
ES_EVENT_INDEX = 'wikimedia-edits'

# Elasticsearch client
es = Elasticsearch(ES_HOST)

# Logging setup
logger = logging.getLogger("wikimedia-ingest")
logger.setLevel(logging.INFO)

# Elasticsearch handler for script logs
class ElasticsearchHandler(logging.Handler):
    def emit(self, record):
        log_entry = {
            "message": self.format(record),
            "level": record.levelname,
            "timestamp": datetime.utcnow()
        }
        try:
            es.index(index=ES_LOG_INDEX, document=log_entry)
        except Exception as e:
            print(f"Failed to log to Elasticsearch: {e}")

es_handler = ElasticsearchHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
es_handler.setFormatter(formatter)
logger.addHandler(es_handler)

# --- Functions ---
def get_wikimedia_stream_client():
    try:
        client = sseclient.SSEClient(WIKIMEDIA_STREAM_URL, timeout=5)
        logger.info(f"Connected to Wikimedia stream at {WIKIMEDIA_STREAM_URL}")
        return client
    except requests.exceptions.RequestException as e:
        logger.error(f"Error connecting to Wikimedia stream: {e}", exc_info=True)
        exit(1)

def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            api_version=(0, 10, 1),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info("Kafka producer created successfully.")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}", exc_info=True)
        exit(1)

def log_event_to_es(change: dict):
    """Log the actual Wikimedia edit event to Elasticsearch."""
    try:
        doc = {
            "title": change.get("title"),
            "user": change.get("user"),
            "namespace": change.get("namespace"),
            "type": change.get("type"),
            "timestamp": change.get("timestamp"),
            "meta": change  # store full event
        }
        es.index(index=ES_EVENT_INDEX, document=doc)
    except Exception as e:
        logger.error(f"Failed to log event to Elasticsearch: {e}", exc_info=True)

# --- Main ---
def main():
    logger.info("Script execution started.")
    producer = create_kafka_producer()
    client = get_wikimedia_stream_client()

    logger.info("Initialization complete. Starting event loop...")
    for event in client:
        if event.event == 'message':
            try:
                change = json.loads(event.data)
                if change.get('type') == 'edit' and change.get('namespace') == 0:
                    logger.info(f"Publishing edit for page: {change.get('title')}")
                    producer.send(KAFKA_TOPIC, change)
                    log_event_to_es(change)
            except json.JSONDecodeError:
                logger.warning(f"Could not decode JSON: {event.data}")
            except Exception as e:
                logger.error(f"Unhandled error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()
