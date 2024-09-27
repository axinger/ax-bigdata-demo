package com.github.axinger._09合并combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN: map阶段输入key的类型,
 * VALUEIN: map阶段输入value类型 text,
 * KEYOUT: map阶段输出key类型, text
 * VALUEOUT: map阶段输出value类型,int
 */

public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text outK = new Text();
    private final IntWritable outV = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //key 偏移量
        System.out.println(" map value = " + value + ", map key = " + key);
        //1.获取一行,分割
        String[] words = value.toString().split(" ");
        //循环写出
        for (String word : words) {
            outK.set(word);
            //写出
            context.write(outK, outV);
        }
    }
}
