package com.github.axinger;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class A03RddDisk {

    public static void main(String[] args) {

        final SparkConf conf = new SparkConf().setAppName("A02Env")
                .setMaster("local");
        @Cleanup final JavaSparkContext context = new JavaSparkContext(conf);

        // 读取文件
        JavaRDD<String> rdd = context.textFile("./input/word.txt");
        rdd.foreach(val -> {
            System.out.println("val = " + val);
            log.info("val = {}", val);
        });


    }
}
