"""RandomForestClassification.py"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("SQL Census Data A").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([ \
      StructField("age", IntegerType()), \
      StructField("workclass", StringType()), \
      StructField("fnlwgt", IntegerType()), \
      StructField("education", StringType()), \
      StructField("educationNum", IntegerType()), \
      StructField("maritalStatus", StringType()), \
      StructField("occupation", StringType()), \
      StructField("relationship", StringType()), \
      StructField("race", StringType()), \
      StructField("sex", StringType()), \
      StructField("capitalGain", IntegerType()), \
      StructField("capitalLoss", IntegerType()), \
      StructField("hoursPerWeek", IntegerType()), \
      StructField("nativeCountry", StringType()), \
      StructField("income", StringType()) \
])    
data = spark.read.schema(schema).option("header", True).csv("../data/adult.csv").cache()

stringFeatureCols = "workclass maritalStatus occupation relationship race sex".split(" ")
intFeatureCols = "age educationNum capitalGain capitalLoss hoursPerWeek".split(" ")

indexedData = data
for name in stringFeatureCols:
    indexer = StringIndexer().setInputCol(name).setOutputCol(name+"-i")
    indexedData = indexer.fit(indexedData).transform(indexedData)
indexedData = indexedData.withColumn("label", F.when(indexedData.income == '>50K', 1).otherwise(0))
indexedData.show()
assembler = VectorAssembler(). \
    setInputCols(intFeatureCols + list(map(lambda s: s+"-i", stringFeatureCols))). \
    setOutputCol("features")
assembledData = assembler.transform(indexedData)
assembledData.show()

splitData = assembledData.randomSplit([0.8, 0.2])
rf = RandomForestClassifier()
model = rf.fit(splitData[0])

predictions = model.transform(splitData[1])
predictions.show()
evaluator = BinaryClassificationEvaluator()
accuracy = evaluator.evaluate(predictions)
print("accuracy = "+str(accuracy))

spark.stop()
