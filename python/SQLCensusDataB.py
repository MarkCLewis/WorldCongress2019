"""SQLCensusDataA.py"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

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

n = data.count()
print("Fraction > 50K = " + str(data.filter("income = '>50K'").count() / n))
print("Average age = " + str(data.agg({"age": "sum"}).collect()[0]["sum(age)"] / n))
over50years = data.filter("age >= 50")
print("Fraction > 50K in 50+ age group = " + str(over50years.filter("income = '>50K'").count() / over50years.count()))
married = data.filter("maritalStatus = 'Married-civ-spouse'")
print("Fraction > 50K in married group = " + str(married.filter("income = '>50K'").count() / married.count()))
print("Quartile age = " + str(data.select("age").stat.approxQuantile("age", [0.25, 0.5, 0.75], 0.1)))
print("Fraction by race")
# I removed the race grouped fractional computation because it requires using a Pandas UDF.
print("Fraction work more than 40 hrs/week = " + str(data.filter("hoursPerWeek > 40").count() / n))

spark.stop()
