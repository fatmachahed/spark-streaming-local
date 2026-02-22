# Write your csv to json transformation code here 

from pyspark.sql.types import StructType, StringType, IntegerType

event_schema = StructType() \
    .add("user_id", StringType()) \
    .add("event_type", StringType()) \
    .add("amount", IntegerType()) \
    .add("timestamp", StringType())