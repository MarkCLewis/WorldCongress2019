package withsql;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import static org.apache.spark.sql.functions.col;

public class JSimpleAppSQL {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Application").master("local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        
        String txtFile = "src/main/java/withsql/JSimpleAppSQL.java";
        Dataset<String> logData = spark.read().textFile(txtFile).cache();
        long numVals = logData.filter(col("value").like("%val%")).count();
        System.out.println("Lines with val: "+numVals);
        spark.stop();            
    }
}