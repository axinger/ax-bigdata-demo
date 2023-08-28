package com.axing._08分区排序;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Arrays;
import java.util.List;

/**
 * map方法输出kv
 */
public class ProvincePartitioner extends Partitioner<FlowBean, Text> {


    private final List<String> list = Arrays.asList("136", "137", "138", "139");


    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        // text是手机号
        String phone = text.toString().substring(0, 3);
        int index = list.indexOf(phone);
        return index > -1 ? index : 4;
    }
}
