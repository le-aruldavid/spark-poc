package org.com.spark;

import org.apache.spark.sql.SparkSession;

public class SparkSQLPOC {
    public static void main(String args[]){
        SparkSession sps = SparkSession.builder().appName("Spark SQL Demo").master("local[*]").getOrCreate();
    }
}
