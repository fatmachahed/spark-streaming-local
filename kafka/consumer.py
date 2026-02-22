# Write your kafka consumer debugger code here 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
import sys
import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)
from utils.schema import event_schema

if os.name == 'nt': 
    hadoop_home = r"C:\hadoop"
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] = os.path.join(hadoop_home, "bin") + ";" + os.environ["PATH"]

TOPIC = "events-topic"
BOOTSTRAP_SERVERS = "localhost:9092"

spark = SparkSession.builder \
    .appName("SparkKafkaStreamingLab") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1️⃣ Read from Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# 2️⃣ Convert binary → string
df_string = df_kafka.select(
    col("value").cast("string").alias("json_value")
)

# 3️⃣ Parse JSON
df_parsed = df_string.select(
    from_json(col("json_value"), event_schema).alias("data")
).select("data.*")

# 4️⃣ Filter (example: only high purchases)
df_filtered = df_parsed.filter(
    (col("event_type") == "purchase") & (col("amount") > 100)
)

# 5️⃣ Write to console
query = df_filtered.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()