package withrdd;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import utility.JCensusData;

public class JRDDCensusData {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");

	    JavaRDD<String> csvFileLines = sc.textFile("../data/adult.csv", 2).filter(line -> !line.contains("age"));
	    JavaRDD<JCensusData> data = csvFileLines.map(JCensusData::parseLine).cache();

	    long n = data.count();
	    System.out.println("Fraction > 50K = " + data.filter(cd -> cd.incomeOver50).count() / (double)n);
	    System.out.println("Average age = " + JavaDoubleRDD.fromRDD(JavaRDD.toRDD(data.map(cd -> (double)cd.age))).sum() / (double)n);
	    JavaRDD<JCensusData> over50years = data.filter(cd -> cd.age >= 50);
	    System.out.println("Fraction > 50K in 50+ age group = " + over50years.filter(cd -> cd.incomeOver50).count() / (double)over50years.count());
	    JavaRDD<JCensusData> married = data.filter(cd -> cd.maritalStatus == "Married-civ-spouse");
	    System.out.println("Fraction > 50K in married group = " + married.filter(cd -> cd.incomeOver50).count() / (double)married.count());
	    System.out.println("Median age = " + data.sortBy(cd -> cd.age, true, data.partitions().size()).zipWithIndex().filter(t -> t._2 == n / 2).collect().get(0)._1.age);
	    System.out.println("Fraction by race");
	    JavaPairRDD<String, JCensusData> pairRDD = JavaPairRDD.fromJavaRDD(data.map(cd -> new Tuple2(cd.race, cd)));
	    List<Tuple2<String, Tuple2<Integer, Integer>>> raceCounts = pairRDD.aggregateByKey(new Tuple2(0,0),
	    				(Tuple2<Integer, Integer> tup, JCensusData cd) -> new Tuple2(tup._1 + 1, tup._2 + ((cd.incomeOver50) ? 1 : 0)),
	    				(Tuple2<Integer, Integer> tup1, Tuple2<Integer, Integer> tup2) -> new Tuple2(tup1._1 + tup2._1, tup1._2 + tup2._2)).collect();
	    for (Tuple2<String, Tuple2<Integer, Integer>> tup: raceCounts) {
	    	String race = tup._1;
	    	int tot = tup._2._1;
	    	int over = tup._2._2;
	    	System.out.println("  "+race+" = "+(over / (double)tot));
	    }
	    System.out.println("Fraction work more than 40 hrs/week = " + data.filter(cd -> cd.hoursPerWeek > 40).count() / (double)n);

		sc.stop();
	}
}
