## Spark Streaming — Kafka to Spark Pipeline (Introduction Lab)

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, upper

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

from utils.schema import event_schema

if os.name == 'nt':
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

TOPIC = "events-topic"
BOOTSTRAP_SERVERS = "localhost:9092"

def main():

    # TODO 2: Create a SparkSession
    spark = SparkSession.builder \
        .appName("SparkKafkaStreamingLab") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # TODO 3: Define Kafka connection parameters
    kafka_options = {
        "kafka.bootstrap.servers": BOOTSTRAP_SERVERS,
        "subscribe": TOPIC,
        "startingOffsets": "latest"
    }

    # TODO 4: Read data from Kafka as a streaming DataFrame
    df_kafka = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()

    # TODO 5: Inspect the schema of the streaming DataFrame
    df_kafka.printSchema()
    # Schema Kafka : key, value (binary), topic, partition, offset, timestamp, timestampType

    # TODO 6: Convert the Kafka message value from bytes to string
    df_string = df_kafka.select(
        col("value").cast("string").alias("json_value")
    )

    # TODO 7: Apply a simple transformation
    # Parse JSON puis transformer event_type en majuscules
    df_parsed = df_string.select(
        from_json(col("json_value"), event_schema).alias("data")
    ).select("data.*")

    df_transformed = df_parsed.withColumn(
        "event_type", upper(col("event_type"))
    )

    # TODO 8: Write the streaming output
    query = df_transformed.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    # TODO 9: Keep the streaming query running
    query.awaitTermination()

    # TODO 10: Gracefully stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
