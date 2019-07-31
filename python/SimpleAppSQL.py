"""SimpleAppSQL.py"""
from pyspark.sql import SparkSession

txtFile = "SimpleAppSQL.py"
spark = SparkSession.builder.appName("Simple Application SQL").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
logData = spark.read.text(txtFile).cache()

numSparks = logData.filter(logData.value.contains("spark")).count()

print("Lines with spark: %i" % (numSparks))

spark.stop()