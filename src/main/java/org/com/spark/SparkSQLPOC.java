package org.com.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLPOC {
    public static void main(String args[]){
        SparkSession sps = SparkSession.builder().appName("Spark SQL Demo").master("local[*]").getOrCreate();
        Dataset<Row> peopleSet = sps.read().json("C:\\Development\\spark\\SparkQuickDemo\\SparkQuickDemo\\src\\main\\java\\org\\com\\spark\\people.json");
        peopleSet.show();

    }
}
