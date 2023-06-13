from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

# Create a SparkSession
spark = SparkSession.builder.appName("KafkaSparkIntegration").getOrCreate()

# Define the schema for the messages in Kafka
schema = StructType([
    StructField("DO", FloatType()),
    StructField("pH", FloatType()),
    StructField("ORP", FloatType()),
    StructField("Cond", FloatType()),
    StructField("Temp", FloatType()),
    StructField("WQI", FloatType())
])

# Read data from Kafka as a DataFrame
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sangam") \
    .option("failOnDataLoss", 'False')\
    .load()

# Parse the value field from Kafka as JSON and apply the schema
parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Print the DataFrame to the console
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option('path', 'output')\
    .option('checkpointLocation','checkpoint')\
    .start()

query.awaitTermination()
