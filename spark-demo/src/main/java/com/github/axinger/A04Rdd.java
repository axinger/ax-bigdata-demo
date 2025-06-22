package com.github.axinger;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple1;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class A04Rdd {

    public static void main(String[] args) {

        final SparkConf conf = new SparkConf().setAppName("A02Env")
                .setMaster("local")
                ;
        @Cleanup final JavaSparkContext context = new JavaSparkContext(conf);


        List<String> list = Arrays.asList("1", "2");

        JavaRDD<String> rdd = context.parallelize(list);


        // collect 会导致,多个executor 之间产生数据交互,性能会下降,生产一般不使用
        List<String> collect = rdd.map(line -> line + "A").collect();
        System.out.println("collect = " + collect);

        long count = rdd.count();
        System.out.println("count = " + count);


        Tuple1<String> tuple1 = Tuple1.apply("1");
        System.out.println("tuple1 = " + tuple1._1);

        Tuple2<String, Integer> tuple2 = Tuple2.apply("a", 2);
        System.out.println("tuple2 = " + tuple2._2);
    }
}
