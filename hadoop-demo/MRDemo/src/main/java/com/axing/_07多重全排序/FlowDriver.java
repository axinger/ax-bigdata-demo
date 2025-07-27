package com.axing._07多重全排序;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //2设置jar路径
        job.setJarByClass(FlowDriver.class);

        //3关联map和reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4.设置map输出类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //5.设置最终输出类型,不是reducer的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        //6.设置输入路径,和输出路径
        FileInputFormat.setInputPaths(job, new Path("./input/phone04"));
        FileOutputFormat.setOutputPath(job, new Path("./out/phone07"));


        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.out.println("提交job result = " + result);
        System.exit(result ? 0 : 1);
    }
}
