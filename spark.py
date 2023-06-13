from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from pyspark.ml.classification import LogisticRegression
import csv

spark = SparkSession.builder.appName("KafkaSparkIntegration").getOrCreate()

schema = StructType([
    StructField("DO", FloatType()),
    StructField("pH", FloatType()),
    StructField("ORP", FloatType()),
    StructField("Cond", FloatType()),
    StructField("Temp", FloatType()),
    StructField("WQI", FloatType()),
    StructField("Status",StringType())
])

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer

 
dataStream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sangam") \
    .option("failOnDataLoss", 'False')\
    .load()\



# data = spark.read.csv("sangam.csv",header=True,inferSchema='True')
# ind = StringIndexer(inputCol = 'Status', outputCol = 'Status_index')
# data=ind.fit(data).transform(data)
# numericCols = ['DO', 'pH', 'ORP', 'Cond','Temp','WQI']
# assembler = VectorAssembler(inputCols=numericCols, outputCol="features")
# data = assembler.transform(data).select("features", "Status_index")

# lr = LogisticRegression(featuresCol = 'features', labelCol = 'Status_index',maxIter=50, regParam=0.01, elasticNetParam=0.0)
# lrModel = lr.fit(data.select('features'))

#def process_row(row):
#        with open ('./o.csv', 'a') as f:
 #           writer = csv.writer(f)
#          writer.writerow(row=row)
            

#query = dataStream.writeStream.foreach(process_row).start().awaitTermination()

# outputStream = lrModel.transform(testingData) \
#   .selectExpr("CAST(label AS STRING) AS key", "to_json(struct(*)) AS value") \
#   .writeStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "localhost:9092") \
#   .option("topic", "output-topic") \
#   .option("checkpointLocation", "checkpoint") \
#   .start()