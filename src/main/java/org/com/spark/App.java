package org.com.spark;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.LongAccumulator;
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

        //Map Partition example
        JavaRDD<Integer> partList = distData.mapPartitions((Iterator<Integer> it) -> {
            ArrayList<Integer> out = new ArrayList<>();
            while(it.hasNext()) {
                Integer current = it.next();
                out.add(current+1);
            }
            return out.iterator();
        });

        partList.foreach( m -> {
            System.out.println(m);
        });

        System.out.println("Sample Count");
        partList.sample(false,0.4,123).collect().forEach(n -> {
            System.out.println(n);
        });

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
        //Group By Key Example
        JavaPairRDD<String, Iterable<Integer>> groupRDD = linesPairRDD.groupByKey();
        groupRDD.collect().forEach(new Consumer<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void accept(Tuple2<String, Iterable<Integer>> stringIterableTuple2) {
                Iterator<Integer> it = stringIterableTuple2._2().iterator();
                int sum =0;
                while(it.hasNext()){
                    sum = sum + it.next();

                }
                //System.out.println(stringIterableTuple2._1 + " -- "+sum);
            }
        });

        //JavaPairRDD<String,Integer> redPairRDD = linesPairRDD.reduceByKey((a,b) -> (a+b));
        /*redPairRDD.collect().forEach(new Consumer<Tuple2<String, Integer>>() {
            @Override
            public void accept(Tuple2<String, Integer> stringIntegerTuple2) {
                System.out.println(stringIntegerTuple2._1 + " -->"+stringIntegerTuple2._2);
            }
        });*/

        /*//Union Set
        List<String> data1 = Arrays.asList("abc", "abc", "xyz");
        JavaRDD<String> dataRDD=jsc.parallelize(data1);
        List<String> data2 = Arrays.asList("rrr", "mmm", "nnn","ggf","abc");
        JavaRDD<String> data2RDD=jsc.parallelize(data2);
        JavaRDD<String> combinedRDD = dataRDD.union(data2RDD);
        JavaRDD<String> interRDD = dataRDD.intersection(data2RDD);
        combinedRDD.collect().forEach(s -> {
            System.out.println(s);
        });
        System.out.println("aIntersection");
        interRDD.collect().forEach(s -> {
            System.out.println(s);
        });*/

        /*System.out.println("Distinct");
        JavaRDD<String> distRDD = dataRDD.distinct();
        distRDD.collect().forEach(s -> {
            System.out.println(s);
        });*/

        //Co-group or groupWith example
        List<String> s1 = Arrays.asList("Apple","Orange","Pineapple","Grapes");
        List<String> s2 = Arrays.asList("Lyches","Dates","Pineapple","Grapes");
        JavaPairRDD<String,Integer> s11 = jsc.parallelize(s1).mapToPair( m -> new Tuple2<String,Integer>(m,1));
        JavaPairRDD<String,Integer> s22 = jsc.parallelize(s2).mapToPair( n -> new Tuple2<String,Integer>(n,1));

        System.out.println("CO Group");
        JavaPairRDD<String,Tuple2<Iterable<Integer>,Iterable<Integer>>> coGroupRDD = s11.cogroup(s22);

        coGroupRDD.take(3).forEach(new Consumer<Tuple2<String, Tuple2<Iterable<Integer>, Iterable<Integer>>>>() {
            @Override
            public void accept(Tuple2<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> stringTuple2Tuple2) {
                int sum1=0;
                int sum2=0;
                Iterator<Integer> it1 = stringTuple2Tuple2._2()._1().iterator();
                Iterator<Integer> it2 = stringTuple2Tuple2._2()._2().iterator();
                while(it1.hasNext()){
                    sum1 = sum1 + it1.next();
                }
                while(it2.hasNext()){
                    sum2 = sum2 + it2.next();
                }

                System.out.println(stringTuple2Tuple2._1() + " "+sum1 + " "+sum2);
            }
        });

        //Broadcast variables
        Broadcast<String> bc = jsc.broadcast("Welcome to broadcast variable");
        System.out.println(bc.value());
        //Unpersist the broadcast variable
        bc.unpersist(true);
        System.out.println(bc.value());

        //Destroy the broadcast variable
        bc.destroy(true);
        //System.out.println(bc.value());

        //Accumulator for counters
        Accumulator<Integer> lac = jsc.accumulator(10000);
        lac.add(50);
        System.out.println(lac.value());


    }


}

class GetLength implements Function<String, Integer> {
    public Integer call(String s) { return s.length(); }
}

class Sum implements Function2<Integer, Integer, Integer> {
    public Integer call(Integer a, Integer b) { return a + b; }
}

