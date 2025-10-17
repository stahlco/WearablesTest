# Use Strimzi Connect base image as a starting point. Replace <strimzi-connect-image> with the image your operator expects.
FROM quay.io/strimzi/kafka:0.48.0-kafka-4.1.0

USER root

# Create plugin path used by connect
RUN mkdir -p /opt/kafka/plugins/stream-reactor

# Copy the stream reactor plugin files (you must have these locally)
COPY kafka-connect-mqtt-assembly-10.0.1.jar /opt/kafka/plugins/stream-reactor/

# Ensure proper permissions
RUN chown -R 1001:0 /opt/kafka/plugins && chmod -R g+rw /opt/kafka/plugins

# Expose same settings as original image; entrypoint remains the same (Strimzi image default)
USER 1001
