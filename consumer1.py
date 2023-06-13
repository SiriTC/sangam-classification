from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *


spark = SparkSession.builder.appName("KafkaSparkIntegration").getOrCreate()


schema = StructType([
    StructField("DO", FloatType()),
    StructField("pH", FloatType()),
    StructField("ORP", FloatType()),
    StructField("Cond", FloatType()),
    StructField("Temp", FloatType()),
    StructField("WQI", FloatType())
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sangam") \
    .option("failOnDataLoss", 'False')\
    .load()\
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.DO","data.pH","data.ORP","data.Cond","data.Temp","data.WQI")\

query = kafka_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option('path', 'output')\
    .option('checkpointLocation','checkpoint')\
    .start()

query.awaitTermination()