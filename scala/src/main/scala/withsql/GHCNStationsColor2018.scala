package withsql

import org.apache.spark.sql.SparkSession
import utility.GHCNStation
import utility.GHCNData
import swiftvis2.plotting.Plot
import swiftvis2.plotting.styles.ScatterStyle
import swiftvis2.plotting.renderer.SwingRenderer
import org.apache.spark.sql.functions._
import swiftvis2.plotting.ColorGradient

object GHCNStationsColor2018 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("GHCN Color Application").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val stations = GHCNStation.readToTypedDataset(spark, "../data/ghcnd-stations.txt").cache
    val data = GHCNData.readToTypedDataset(spark, "../data/2018.csv")
    
    stations.show()
    data.show()
    
    val highTemps = data.filter(_.element == "TMAX")
    val stationAverages = highTemps.groupBy("id")
      .agg((avg($"value")/10*1.8+32).as("avgTempF"))
    stationAverages.show()
    
    val joinedData = stationAverages.join(stations, "id")
    joinedData.show()
    
    val x = joinedData.select($"lon").collect.map(_.getAs[Double](0))
    val y = joinedData.select($"lat").collect.map(_.getAs[Double](0))
    val cg = ColorGradient.apply(0.0 -> 0xff0000ff, 100.0 -> 0xffff0000)
    val temps = joinedData.select($"avgTempF").collect.map(_.getAs[Double](0))
    val plot = Plot.simple(ScatterStyle.apply(x, y, colors = cg(temps)), "Stations", "Longitude", "Latitude")
    SwingRenderer(plot, 1000, 1000, true) 
    
    spark.stop()
  }
}