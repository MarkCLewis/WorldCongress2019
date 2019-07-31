"""RDDCensusData.py"""
from pyspark import SparkContext, SparkConf
import CensusData

def skip1AtIndex0(idx, iter):
    if idx==0:
        next(iter)
    return iter

conf = SparkConf().setAppName("Simple Application").setMaster("local[*]")
sc = SparkContext(conf=conf)

sc.setLogLevel("WARN")

csvFileLines = sc.textFile("../data/adult.csv", 2).mapPartitionsWithIndex(skip1AtIndex0)
data = csvFileLines.map(CensusData.parseLine).cache()

n = data.count()
print("Fraction > 50K = " + str(data.filter(lambda cd: cd.incomeOver50).count() / n))
print("Average age = " + str(data.map(lambda cd: cd.age).sum() / n))
over50years = data.filter(lambda cd: cd.age >= 50)
print("Fraction > 50K in 50+ age group = " + str(over50years.filter(lambda cd: cd.incomeOver50).count() / over50years.count()))
married = data.filter(lambda cd: cd.maritalStatus == "Married-civ-spouse")
print("Fraction > 50K in married group = " + str(married.filter(lambda cd: cd.incomeOver50).count() / married.count()))
print("Median age = " + str(data.sortBy(lambda cd: cd.age).zipWithIndex().filter(lambda t: t[1] == n // 2).collect()[0][0].age))
print("Fraction by race")
raceCounts = data.map(lambda cd: (cd.race, cd)).aggregateByKey((0, 0), \
    lambda tup, cd: (tup[0]+1, tup[1]+(1 if cd.incomeOver50 else 0)), \
    lambda tup0, tup1: (tup0[0] + tup1[0], tup0[1] + tup1[1])).collect()
for tup in raceCounts:
    race = tup[0]
    tot = tup[1][0]
    over = tup[1][1]
    print("  "+str(race)+" = "+str(over / tot))
print("Fraction work more than 40 hrs/week = " + str(data.filter(lambda cd: cd.hoursPerWeek > 40).count() / n))

sc.stop()