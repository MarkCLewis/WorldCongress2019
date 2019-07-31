package machlearn;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JKMeansClustering {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Simple Application").master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("WARN");

	    Dataset<Row> csvData = spark.read().option("header", true).csv("../data/LLCP2015.csv");
	    	    
	    String[] columnsToKeep = "GENHLTH PHYSHLTH MENTHLTH POORHLTH EDUCA SEX MARITAL EMPLOY1".split(" ");
	    	    
	    Dataset<Row> typedData = csvData;
	    for (String colName: columnsToKeep) {
	    	typedData = typedData.withColumn(colName, col(colName).cast("int").as(colName)).na().drop();
	    }
	    VectorAssembler assembler = new VectorAssembler().setInputCols(columnsToKeep).setOutputCol("features");
	    Dataset<Row> dataWithFeatures = assembler.transform(typedData);
	    	    
	    Normalizer normalizer = new Normalizer().setInputCol("features").setOutputCol("normFeatures");
	    Dataset<Row> normData = normalizer.transform(dataWithFeatures);
	    	    
	    KMeans kmeans = new KMeans().setK(5).setFeaturesCol("normFeatures");
	    KMeansModel model = kmeans.fit(normData);
	    	    
	    Dataset<Row> predictions = model.transform(normData);
	    predictions.select("features", "prediction").show();

	    ClusteringEvaluator evaluator = new ClusteringEvaluator();

	    double silhouette = evaluator.evaluate(predictions);
	    System.out.println("Silhouette with squared euclidean distance = " + silhouette);
	    
	    spark.stop();
	}
}
