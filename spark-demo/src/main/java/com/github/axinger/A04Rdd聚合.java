package com.github.axinger;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
public class A04Rdd聚合 {

    public static void main(String[] args) {

        final SparkConf conf = new SparkConf().setAppName("A02Env")
                .setMaster("local");
        @Cleanup final JavaSparkContext context = new JavaSparkContext(conf);


        List<String> list = Arrays.asList("张三", "张三", "李四");

        JavaRDD<String> rdd = context.parallelize(list);

        //  Tuple2<K, V> call(T var1) throws Exception;
        JavaPairRDD<String, Integer> reduceByKey = rdd.mapToPair(line -> new Tuple2<>(line, 1))
                .reduceByKey(Integer::sum);
        System.out.println("reduceByKey = " + reduceByKey.take(10));

        //countByValue 是一种更简洁的方式，它会直接统计每个元素出现的次数：
        Map<String, Long> countByValue = rdd.countByValue();
        System.out.println("countByValue = " + countByValue);


    }
}
