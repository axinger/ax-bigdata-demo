package com.github.axinger;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class A02RddMemoryPartition {

    public static void main(String[] args) {

        // 3. 创建 Spark 配置
        SparkConf conf = new SparkConf()
                .setAppName("SparkLocalTest")
//                .setMaster("local[2]")  // 使用所有核心
                .setMaster("local")  // 使用所有核心
                ;
        List<String> list = Arrays.asList("1", "2","3","4");
        // local[2]:  分区数量,一般需要手动设置
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            sc.parallelize(list,3) //分区数量
                    .saveAsTextFile("output");
        }
    }
}
