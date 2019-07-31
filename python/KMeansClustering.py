"""KMeansClustering.py"""
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import VectorAssembler, Normalizer
from pyspark.ml.clustering import KMeans

# Currently not working. Calls to withColumn don't seem to be doing the right thing.

spark = SparkSession.builder.appName("SQL Census Data A").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

csvData = spark.read.option("header", True).csv("../data/LLCP2015.csv")

columnsToKeep = "GENHLTH PHYSHLTH MENTHLTH POORHLTH EDUCA SEX MARITAL EMPLOY1".split(" ")
typedData = csvData

for colName in columnsToKeep:
    typedData = csvData.withColumn(colName, typedData[colName].cast(IntegerType()).alias(colName))

typedData = typedData.na.drop()
print(typedData.schema)

assembler = VectorAssembler().setInputCols(columnsToKeep).setOutputCol("features")
dataWithFeatures = assembler.transform(typedData)
dataWithFeatures.show()

normalizer = Normalizer().setInputCol("features").setOutputCol("normFeatures")
normData = normalizer.transform(dataWithFeatures)

kmeans = KMeans().setK(5).setFeaturesCol("normFeatures")
model = kmeans.fit(normData)

predictions = model.transform(normData)
predictions.select("features", "prediction").show()

evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

spark.stop()
