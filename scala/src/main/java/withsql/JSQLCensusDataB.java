package withsql;

import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

public class JSQLCensusDataB {
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

		long n = data.count();
		System.out.println("Fraction > 50K = " + data.filter(col("income").equalTo(">50K")).count() / (double) n);
		System.out.println("Average age = " + data.agg(avg(col("age"))).collectAsList().get(0));
		System.out.println("Age stats = " + data.describe("age").collectAsList());
		Dataset<Row> over50years = data.filter(col("age").geq(50));
		System.out.println("Fraction > 50K in 50+ age group = "
				+ over50years.filter(col("income").equalTo(">50K")).count() / (double) over50years.count());
		Dataset<Row> married = data.filter(col("maritalStatus").equalTo("Married-civ-spouse"));
		System.out.println("Fraction > 50K in married group = "
				+ married.filter(col("income").equalTo(">50K")).count() / (double) married.count());
		System.out
				.println("Quartile age = " + data.stat().approxQuantile("age", new double[] { 0.25, 0.5, 0.75 }, 0.1));
		System.out.println("Fraction by race");
		Dataset<Row> raceCounts = data.groupBy(col("race")).agg(count(col("income")),
				count(when(col("income").equalTo(">50K"), true)));
		for (Row row : raceCounts.collectAsList()) {
			System.out.println(row + " " + row.getLong(2) / row.getLong(1));
		}
		System.out.println("Fraction work more than 40 hrs/week = "
				+ data.filter(col("hoursPerWeek").gt(40)).count() / (double) n);

		spark.stop();
	}
}