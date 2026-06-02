#!/usr/bin/env bash
# Flink is a Java application. Even though we wrote SQL, Flink still needs Java
# connector libraries (.jar files) to physically talk to Kafka and PostgreSQL.
#
# The base Flink Docker image ships without these — you only pull in what your job needs.
# This script downloads the four JARs our job requires from Maven Central (the standard
# Java package registry) and saves them into flink/jars/.
#
# Run once before docker-compose up. docker-compose.yml mounts each JAR directly
# into /opt/flink/lib/ inside the Flink containers — the files must exist locally first.
#
#   bash flink/download_jars.sh
#   docker-compose up -d

set -e

JARS_DIR="$(dirname "$0")/jars"
mkdir -p "$JARS_DIR"

BASE="https://repo1.maven.org/maven2"

echo "Downloading Kafka connector..."
curl -L -o "$JARS_DIR/flink-connector-kafka-3.1.0-1.18.jar" \
  "$BASE/org/apache/flink/flink-connector-kafka/3.1.0-1.18/flink-connector-kafka-3.1.0-1.18.jar"

# The Flink Kafka connector does not bundle the Kafka client library itself.
# kafka-clients provides the actual Kafka consumer/producer implementation.
echo "Downloading Kafka client..."
curl -L -o "$JARS_DIR/kafka-clients-3.6.0.jar" \
  "$BASE/org/apache/kafka/kafka-clients/3.6.0/kafka-clients-3.6.0.jar"

echo "Downloading JDBC connector..."
curl -L -o "$JARS_DIR/flink-connector-jdbc-3.1.2-1.18.jar" \
  "$BASE/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar"

echo "Downloading PostgreSQL driver..."
curl -L -o "$JARS_DIR/postgresql-42.7.3.jar" \
  "$BASE/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"

echo "Done. JARs saved to $JARS_DIR"
echo "Next: docker-compose up -d"
