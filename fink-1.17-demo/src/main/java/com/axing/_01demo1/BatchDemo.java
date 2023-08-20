package com.axing._01demo1;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

// 批次处理
public class BatchDemo {
    public static void main(String[] args) throws Exception {

        //执行环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        //读取文件
        DataSource<String> read = environment.readTextFile("12.txt");


        //切分,转换
        FlatMapOperator<String, Tuple2<String, Integer>> map = read.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = value.split(" ");
            for (String word : words) {
                Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                out.collect(tuple2);
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT)); //lambda报错,可以在转换的算子之后调用returns(...)方法来显示指明要返回的数据类型信息。

        //按照第一个元素分组
        UnsortedGrouping<Tuple2<String, Integer>> groupedBy = map.groupBy(0);
        //按照第二个元素聚合
        AggregateOperator<Tuple2<String, Integer>> sum = groupedBy.sum(1);
        //输出
        sum.print();

    }
}
