package com.axing._14数据压缩;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WcDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.获取job
        Configuration conf = new Configuration();


        // 开启map端输出压缩
        // map压缩, Reducer会解压,所以取消Reducer才能看见
        conf.setBoolean("mapreduce.map.output.compress", true);
//
//        // 设置map端输出压缩方式
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
//        conf.setClass("mapreduce.map.output.compress.codec", SnappyCodec.class, CompressionCodec.class);


        Job job = Job.getInstance(conf);

        //2设置jar路径
        job.setJarByClass(WcDriver.class);

        //3关联map和reducer
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        //4.设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.设置最终输出类型,不是reducer的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setNumReduceTasks(0);

        // 设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);
//
//        // 设置压缩的方式
//        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
	    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//        FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        //6.设置输入路径,和输出路径
        // 输入目录下,所有的文件都会计算
        FileInputFormat.setInputPaths(job, new Path("./input/01wc"));
        FileOutputFormat.setOutputPath(job, new Path("./out/14yasuo"));


        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
