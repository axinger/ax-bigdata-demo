package com.github.axinger;

import lombok.Cleanup;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class A02RddMemory {

    public static void main(String[] args) {

        final SparkConf conf = new SparkConf().setAppName("A02Env")
                .setMaster("local");
        @Cleanup final JavaSparkContext context = new JavaSparkContext(conf);
        List<String> list = Arrays.asList("1", "2");

        // 并行
        JavaRDD<String> rdd = context.parallelize(list);
        rdd.foreach(val -> {
            System.out.println("val = " + val);
            log.info("val = {}", val);
        });
        // 收集
        List<String> collect = rdd.collect();
        collect.forEach(val -> {
            System.out.println("val2 = " + val);
        });


    }
}
