import logging
import json
import os
from datetime import datetime

# Read the service name from an environment variable.
# Provide a default value for safety.
SERVICE_NAME = os.getenv('SERVICE_NAME', 'unknown-service')

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "message": record.getMessage(),
            "service": SERVICE_NAME, # Use the variable read from the environment
            
            # Extra context for easier debugging
            "module": record.module,
            "funcName": record.funcName,
            "lineno": record.lineno
        }
        # If the log record contains exception information, add it
        if record.exc_info:
            log_record['exc_info'] = self.formatException(record.exc_info)
            
        return json.dumps(log_record)

def setup_logging():
    """Configures the root logger for JSON output. This is now reusable."""
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    if logger.hasHandlers():
        logger.handlers.clear()
        
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)