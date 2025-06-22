package com.github.axinger;

import lombok.Cleanup;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class A01Env {
    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setAppName("A01Env")
                .setMaster("local")
//                .setMaster("local[2]") //2个线程
                ;
        @Cleanup final JavaSparkContext context = new JavaSparkContext(conf);


    }
}
