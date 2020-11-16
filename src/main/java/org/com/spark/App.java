package org.com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

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
        JavaRDD<String> lines = jsc.textFile("C:\\Development\\data\\annual.csv");
        JavaRDD<Integer> lengthLines = lines.map(s -> s.length());
        int count = lengthLines.reduce((a,b) -> (a+b));
        System.out.print(count);

        //Try with inline functions
        JavaRDD<Integer> linesRDD = lines.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });

        int counts = linesRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

        System.out.println(counts);

        //Using the inner class
        JavaRDD<Integer> lineLengths = lines.map(new GetLength());
        int totalLength = lineLengths.reduce(new Sum());
        System.out.println(totalLength);

        //Using the RDD Pairs
        JavaRDD<String> wordsFromFile = lines.flatMap(content -> Arrays.asList(content.split(",")).iterator());
        JavaPairRDD<String,Integer> linesPairRDD = wordsFromFile.mapToPair(s -> new Tuple2<String,Integer>(s,1));

        JavaPairRDD<String,Integer> redPairRDD = linesPairRDD.reduceByKey((a,b) -> (a+b));
        redPairRDD.collect().forEach(new Consumer<Tuple2<String, Integer>>() {
            @Override
            public void accept(Tuple2<String, Integer> stringIntegerTuple2) {
                System.out.println(stringIntegerTuple2._1 + " -->"+stringIntegerTuple2._2);
            }
        });
    }
}

class GetLength implements Function<String, Integer> {
    public Integer call(String s) { return s.length(); }
}

class Sum implements Function2<Integer, Integer, Integer> {
    public Integer call(Integer a, Integer b) { return a + b; }
}