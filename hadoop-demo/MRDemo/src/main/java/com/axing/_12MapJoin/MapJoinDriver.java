package com.axing._12MapJoin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

// map阶段 jon,使用缓存
//Map Join适用于一张表十分小、一张表很大的场景。
public class MapJoinDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(MapJoinDriver.class);
        job.setMapperClass(MapJoinMapper.class);
//        job.setReducerClass(TableReducer.class);

        job.setNumReduceTasks(0); //取消Reducer

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class); // 最终输出泛型

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        //缓存文件
        job.addCacheFile(new URI("./input/11ReducerJoin/pd.txt"));

        FileInputFormat.setInputPaths(job, new Path("./input/11ReducerJoin/order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./out/12MapJoin"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}

