# Start from the official Bitnami Zookeeper image
FROM bitnami/zookeeper:latest

# Switch to the root user to install packages
USER root

# The Bitnami image is Debian-based, so we use apt-get.
# Install netcat-openbsd, which provides the 'nc' command.
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

# Switch back to the non-root user that the Bitnami image uses
USER 1001