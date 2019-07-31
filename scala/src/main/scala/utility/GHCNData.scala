package utility;

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

case class GHCNData(
  id:      String,
  date:    String,
  element: String,
  value:   Int,
  mFlag:   String,
  qFlag:   String,
  sFlag:   String,
  obsTime: String)

object GHCNData {
  def apply(line: String): GHCNData = {
    val p = line.split(",").padTo(8, "")
    GHCNData(p(0), p(1), p(2), p(3).toInt, p(4), p(5), p(6), p(7))
  }

  def readToRDD(sc: SparkContext, file: String): RDD[GHCNData] = {
    sc.textFile(file).map(apply)
  }
  
  def readToTypedDataset(spark: SparkSession, file: String): Dataset[GHCNData] = {
    import spark.implicits._
    spark.read.textFile(file).map(apply)
  }
  
  val schema = StructType(Array(
      StructField("id", StringType),    
      StructField("date", StringType),    
      StructField("element", StringType),    
      StructField("value", IntegerType),    
      StructField("mFlag", StringType),    
      StructField("qFlag", StringType),    
      StructField("sFlag", StringType),    
      StructField("obsTime", StringType)    
    ))
  
  def readToDataFrame(spark: SparkSession, file: String): DataFrame = {
    import spark.implicits._
    spark.read.schema(schema).csv(file)    
  }
}