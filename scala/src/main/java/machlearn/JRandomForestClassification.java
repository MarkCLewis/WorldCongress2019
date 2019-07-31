package machlearn;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

import java.util.Arrays;
import java.util.stream.Stream;

import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class JRandomForestClassification {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Simple Application").master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("WARN");

		StructType schema = DataTypes
				.createStructType(Arrays.asList(DataTypes.createStructField("age", DataTypes.IntegerType, true),
						DataTypes.createStructField("workclass", DataTypes.StringType, true),
						DataTypes.createStructField("fnlwgt", DataTypes.IntegerType, true),
						DataTypes.createStructField("education", DataTypes.StringType, true),
						DataTypes.createStructField("educationNum", DataTypes.IntegerType, true),
						DataTypes.createStructField("maritalStatus", DataTypes.StringType, true),
						DataTypes.createStructField("occupation", DataTypes.StringType, true),
						DataTypes.createStructField("relationship", DataTypes.StringType, true),
						DataTypes.createStructField("race", DataTypes.StringType, true),
						DataTypes.createStructField("sex", DataTypes.StringType, true),
						DataTypes.createStructField("capitalGain", DataTypes.IntegerType, true),
						DataTypes.createStructField("capitalLoss", DataTypes.IntegerType, true),
						DataTypes.createStructField("hoursPerWeek", DataTypes.IntegerType, true),
						DataTypes.createStructField("nativeCountry", DataTypes.StringType, true),
						DataTypes.createStructField("income", DataTypes.StringType, true)));
		Dataset<Row> data = spark.read().schema(schema).option("header", true).csv("../data/adult.csv").cache();

	    String[] stringFeatureCols = "workclass maritalStatus occupation relationship race sex".split(" ");
	    String[] intFeatureCols = "age educationNum capitalGain capitalLoss hoursPerWeek".split(" ");
	    Dataset<Row> indexedData = data;
	    for (String name: stringFeatureCols) {
	    	StringIndexer indexer = new StringIndexer().setInputCol(name).setOutputCol(name+"-i");
	    	indexedData = indexer.fit(indexedData).transform(indexedData);
	    }
	    indexedData = indexedData.withColumn("label", when(col("income").equalTo(">50K"), 1).otherwise(0));
	    indexedData.show();
	    String[] stringFeatureIndexCols = Stream.of(stringFeatureCols).map(s -> s + "-i").toArray(String[]::new);
	    String[] allFeatureCols = Stream.of(intFeatureCols, stringFeatureIndexCols).flatMap(Stream::of).toArray(String[]::new);
	    VectorAssembler assembler = new VectorAssembler().
	    		setInputCols(allFeatureCols).
	    		setOutputCol("features");
	    Dataset<Row> assembledData = assembler.transform(indexedData);
	    assembledData.show();

	    Dataset<Row>[] trainAndValid= assembledData.randomSplit(new double[] {0.8, 0.2});
	    RandomForestClassifier rf = new RandomForestClassifier();
	    RandomForestClassificationModel model = rf.fit(trainAndValid[0]);
	    
	    Dataset<Row> predictions = model.transform(trainAndValid[1]);
	    predictions.show();
	    BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator();
	    double accuracy = evaluator.evaluate(predictions);
	    System.out.println("accuracy = "+accuracy);

		spark.stop();
	}
}
