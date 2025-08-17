# Use an official Python runtime as a parent image
FROM python:3.13-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the shared library first. The build context will be the project root,
# so the path starts from there.
COPY ./shared /app/shared

# Copy the requirements file for this specific service
COPY ./services/ingestion/requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the service's own source code into the container
COPY ./services/ingestion /app

# Command to run when the container launches
CMD ["python", "ingestion.py"]