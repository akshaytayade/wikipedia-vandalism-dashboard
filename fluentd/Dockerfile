# Start from the official Fluentd base image
FROM fluent/fluentd:v1.16-1

# Switch to the root user to install packages and gems
USER root

# The base image is Alpine Linux, which uses the 'apk' package manager.
# We update the package list and install 'build-base' (the equivalent
# of 'build-essential') and 'ruby-dev', which are needed to compile
# some Ruby gems with C-extensions.
RUN apk update && apk add --no-cache \
    build-base \
    ruby-dev

# Now, install the Elasticsearch plugin with verbose output
RUN gem install fluent-plugin-elasticsearch -v 5.3.0 -V --no-document

# Switch back to the default fluent user for better security
USER fluent