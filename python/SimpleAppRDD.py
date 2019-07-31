"""SimpleAppRDD.py"""
from pyspark import SparkContext, SparkConf

txtFile = "SimpleAppRDD.py"
conf = SparkConf().setAppName("Simple Application").setMaster("local[*]")
sc = SparkContext(conf=conf)

sc.setLogLevel("WARN")

txtFileLines = sc.textFile(txtFile, 2).cache()
numVals = txtFileLines.filter(lambda line: "sc" in line).count()
print("Lines with sc: " + str(numVals))

sc.stop()