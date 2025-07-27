package com.axing._04自定义对象序列化;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private final Text outK = new Text();
    private final FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {

        //获取一行
        String line = value.toString();
        String[] words = line.split("\t");

        //抓取想要的数据

        String phone = words[1];
        String up = words[words.length - 3];
        String down = words[words.length - 2];

        //key
        outK.set(phone);

        //value
        outV.setUpFlow(Long.parseLong(up));
        outV.setDownFlow(Long.parseLong(down));
        outV.setSumFlow();

        //写出
        context.write(outK, outV);
    }
}
