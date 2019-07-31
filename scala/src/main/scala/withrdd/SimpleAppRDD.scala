package withrdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Simple example for RDD version of Spark modified from code at:
 *   https://www.nodalpoint.com/development-and-deployment-of-spark-applications-with-scala-eclipse-and-sbt-part-1-installation-configuration/
 */
object SimpleAppRDD {
  def main(args: Array[String]): Unit = {
    val txtFile = "src/main/scala/withrdd/SimpleAppRDD.scala"
    val conf = new SparkConf().setAppName("Simple Application").
      setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val txtFileLines = sc.textFile(txtFile, 2).cache()
    val numVals = txtFileLines.filter(line => line.contains("val")).count()
    println("Lines with val: %d".format(numVals))
    
    val wordCounts = txtFileLines.map(line => line.split(" ").
        count(_.contains("val")))
    val totalWords = wordCounts.reduce(_ + _)
    println(totalWords)
    
    val totalWords2 = txtFileLines.aggregate(0)(
        (cnt, line) => cnt + line.split(" ").count(_.contains("val")),
        _+_)
    println(totalWords2)    
    
    sc.stop()
  }
}