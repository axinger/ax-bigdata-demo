package com.github.axinger;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

@Slf4j
public class A03RddDiskPartition {

    public static void main(String[] args) {

        final SparkConf conf = new SparkConf().setAppName("A02Env")
                .setMaster("local")
                .set("spark.default.parallelism", "1") // 设置默认分区数,取值最小
                ;
        @Cleanup final JavaSparkContext context = new JavaSparkContext(conf);

        // 读取文件,
        // 默认情况下，spark会自动将数据切分为多个partitions，然后分配给多个executor进行计算
        // 有hadoop 实现, hadoop 切片规则
        JavaRDD<String> rdd = context.textFile("./input/word.txt",3);

        rdd.saveAsTextFile("output");


    }
}
