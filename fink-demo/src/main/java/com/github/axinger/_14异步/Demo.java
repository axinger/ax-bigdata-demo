package com.github.axinger._14异步;

import com.github.axinger._08合流.Emp;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Demo {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 7777);


        AsyncDataStream.orderedWait(streamSource, new AsyncFunction<String, String>() {
                    @Override
                    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {

                        List<String> list = new ArrayList<>();
                        list.add(input);
                        resultFuture.complete(list);
                    }
                }, 10, TimeUnit.SECONDS)
                .map(new MapFunction<String, Emp>() {

                    @Override
                    public Emp map(String value) throws Exception {
                        return null;
                    }
                }).print();

    }
}
