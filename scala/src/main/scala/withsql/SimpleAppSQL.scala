package withsql

import org.apache.spark.sql.SparkSession

/**
 * Simple application demonstrating Spark SQL modified from code at:
 *   https://spark.apache.org/docs/latest/quick-start.html
 */
object SimpleAppSQL {
  def main(args: Array[String]): Unit = {
    val txtFile = "src/main/scala/withsql/SimpleAppSQL.scala"
    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val logData = spark.read.textFile(txtFile).cache()
    val numVals = logData.filter(line => line.contains("val")).count()
    println(s"Lines with val: $numVals")
    spark.stop()
  }
}
