package withrdd;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

public class JSimpleAppRDD {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");

		String txtFile = "src/main/java/withrdd/JSimpleAppRDD.java";
		JavaRDD<String> txtFileLines = sc.textFile(txtFile, 2).cache();
		long numVals = txtFileLines.filter(line -> line.contains("val")).count();
		System.out.println(String.format("Lines with val: %d", numVals));

		sc.stop();
	}
}
