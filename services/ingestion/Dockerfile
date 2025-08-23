FROM python:3.11-slim

# Install dependencies
RUN pip install --no-cache-dir \
    sseclient-py \
    requests

# Create log directory
RUN mkdir -p /logs

# Copy ingestion script
COPY ingest.py /app/ingest.py
WORKDIR /app

# Run the ingestion
CMD ["python", "ingest.py"]
