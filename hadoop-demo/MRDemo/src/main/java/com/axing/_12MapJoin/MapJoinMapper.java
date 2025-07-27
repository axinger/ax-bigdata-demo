package com.axing._12MapJoin;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.StrUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;


public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {


    private final Text outK = new Text();

    private Map<String, String> pdMap = new HashMap<>();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 缓存 pd.txt
        URI[] cacheFiles = context.getCacheFiles();
        URI uri = cacheFiles[0];

        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream inputStream = fileSystem.open(new Path(uri));

        BufferedReader reader = IoUtil.getReader(inputStream, StandardCharsets.UTF_8);

        String line;
        while (StrUtil.isNotEmpty(line = reader.readLine())) {
            //切割
            String[] split = line.split("\t");
            pdMap.put(split[0], split[1]);
        }

        IoUtil.close(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        String line = value.toString();

        String[] split = line.split("\t");

        // 封装k  v
        String id = split[0];
        String pid = split[1];
        String amount = split[2];
        String pName = pdMap.getOrDefault(pid, "");
        outK.set(StrUtil.format("{}\t{}\t{}", id, pName, amount));

        // 写出
        context.write(outK, NullWritable.get());
    }
}
