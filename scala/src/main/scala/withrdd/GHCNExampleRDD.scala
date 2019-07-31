package withrdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import utility.GHCNStation
import swiftvis2.plotting.Plot
import swiftvis2.plotting.styles.ScatterStyle
import swiftvis2.plotting.renderer.SwingRenderer

object GHCNExampleRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Census RDD Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val stations = GHCNStation.readToRDD(sc, "../data/ghcnd-stations.txt").cache()
    stations.take(5).foreach(println)
    
    val x = stations.map(_.lon).collect
    val y = stations.map(_.lat).collect
    val plot = Plot.simple(ScatterStyle(x, y), "Stations", "Longitude", "Latitude")
    SwingRenderer(plot, 1000, 1000, true)
  }
}