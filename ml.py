from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession

sql_context = SparkSession.builder.master('local[*]').getOrCreate()

data = sql_context.read.csv("sangam.csv",header=True,inferSchema='True')
from pyspark.ml.feature import StringIndexer

ind = StringIndexer(inputCol = 'Status', outputCol = 'Status_index')
data=ind.fit(data).transform(data)

from pyspark.ml.feature import VectorAssembler

numericCols = ['DO', 'pH', 'ORP', 'Cond','Temp','WQI']
assembler = VectorAssembler(inputCols=numericCols, outputCol="features")

data = assembler.transform(data)


final=data.select("features","Status_index")
train, test = final.randomSplit([0.6, 0.4], seed = 2018)

from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(featuresCol = 'features', labelCol = 'Status_index',maxIter=50, regParam=0.01, elasticNetParam=0.0)
lrmodel = lr.fit(train)
lrmodel.save("lr_model")