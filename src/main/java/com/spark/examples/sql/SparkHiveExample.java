package com.spark.examples.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

public class SparkHiveExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkHive Example");
        SparkContext sc = new SparkContext(conf);
        System.setProperty("hive.metastore.uris", "thrift://localhost:9083");
        HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(sc);
        DataFrame df = hiveContext.sql("show databases");
        df.show();

        DataFrame df2 = hiveContext.sql("show tables in default");
        df2.show();

        DataFrame df1 = hiveContext.sql("select * from default.complex_table");
        df1.show();


    }
}
