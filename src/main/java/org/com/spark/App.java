package org.com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {

        System.out.println( "Spark Java Program!" );
        SparkConf conf = new SparkConf().setAppName("Spark Demo").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5,4,5,666,77,775,554,44,444,566);
        JavaRDD<Integer> distData = jsc.parallelize(data,3);
        System.out.print("Partitions:"+distData.getNumPartitions());

        //Process the Number of lines in a file
        JavaRDD<String> lines = jsc.textFile("C:\\Personal\\DS160\\459139-Print Application.pdf");
        JavaRDD<Integer> lengthLines = lines.map(s -> s.length());
        int count = lengthLines.reduce((a,b) -> (a+b));
        System.out.print(count);
    }
}
