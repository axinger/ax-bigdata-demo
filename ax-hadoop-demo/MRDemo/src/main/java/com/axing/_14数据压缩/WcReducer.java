package com.axing._14数据压缩;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN: Reducer 阶段输入key的类型,
 * VALUEIN: Reducer 阶段输入value类型
 * map的输出
 * <p>
 * KEYOUT: Reducer 阶段输出key类型, text
 * VALUEOUT: Reducer 阶段输出value类型,int
 */

public class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable outV = new IntWritable(1);


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

//        log.info("reduce value={},reduce key={}", values, key);

        //累加
        int sum = 0;
        // hello,(1,1)
        for (IntWritable value : values) {
            sum += value.get();
        }


        outV.set(sum);
        //写出
        context.write(key, outV);
    }
}
