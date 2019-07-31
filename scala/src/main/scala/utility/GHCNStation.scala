package utility;

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame

case class GHCNStation(
    id: String,
    lat: Double,
    lon: Double,
    elevation: Double,
    state: String,
    name: String
)

object GHCNStation {
    def apply(line: String): GHCNStation = {
        val id = line.substring(0, 11).trim
        val lat = line.substring(12, 20).trim.toDouble
        val lon = line.substring(21, 30).trim.toDouble
        val elevation = line.substring(31, 37).trim.toDouble
        val state = line.substring(38, 40).trim
        val name = line.substring(41, 71).trim
        GHCNStation(id, lat, lon, elevation, state, name)
    }

    def readToRDD(sc: SparkContext, file: String): RDD[GHCNStation] = {
        sc.textFile(file).map(apply)
    }
    
    def readToTypedDataset(spark: SparkSession, file: String): Dataset[GHCNStation] = {
      import spark.implicits._
      spark.read.textFile(file).map(apply)
    }
}