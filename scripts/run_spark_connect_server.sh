#!/usr/bin/bash

# The Hadoop and Spark versions are set as environment variables for this script.
echo "The HADOOP_VERSION is $HADOOP_VERSION"
echo "The SPARK_VERSION is $SPARK_VERSION"

# Download the spark binaries. If the download fails, throw an error message
if ! wget -q https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz; then
  echo "Error: Unable to download Spark binaries"
  exit 1
fi

# Extract the downloaded spark binaries and check if the extraction is successful or not
if ! tar -xzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz; then
  echo "Error: Unable to extract Spark binaries"
  exit 1
fi

# Start the Spark server
echo "Starting the Spark-Connect server"
./spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION/sbin/start-connect-server.sh

if [ $? -ne 0 ]; then
  echo "Error: Unable to start the Spark-Connect server"
  exit 1
fi

echo "Spark-Connect server started successfully."