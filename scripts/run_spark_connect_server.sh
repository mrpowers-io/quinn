#!/usr/bin/bash

# This script was inspired by https://github.com/pyspark-ai/pyspark-ai/blob/master/run_spark_connect.sh

# The Spark version is set as an environment variable for this script.
echo "The SPARK_VERSION is $SPARK_VERSION"

# Download the spark binaries. If the download fails, throw an error message
if ! wget -q https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz; then
  echo "Error: Unable to download Spark binaries"
  exit 1
fi

# Extract the downloaded spark binaries and check if the extraction is successful or not
if ! tar -xzf spark-$SPARK_VERSION-bin-hadoop3.tgz; then
  echo "Error: Unable to extract Spark binaries"
  exit 1
fi

# Start the Spark server
echo "Starting the Spark-Connect server"
./spark-$SPARK_VERSION-bin-hadoop3/sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:$SPARK_VERSION

# TODO: Check if the server is running or not (maybe using netstat) and throw an error message if it is not running
