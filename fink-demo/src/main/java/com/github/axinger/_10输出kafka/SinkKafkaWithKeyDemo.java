package com.github.axinger._10输出kafka;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

// 从其他流读取数据，写入kafka中
//自定义序列化器，实现带key的record:
public class SinkKafkaWithKeyDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.noRestart());


        SingleOutputStreamOperator<String> sensorDS = env
                .socketTextStream("hadoop102", 7777);


        /*
         * 如果要指定写入kafka的key，可以自定义序列化器：
         * 1、实现 一个接口，重写 序列化 方法
         * 2、指定key，转成 字节数组
         * 3、指定value，转成 字节数组
         * 4、返回一个 ProducerRecord对象，把key、value放进去
         */
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<String>() {

                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext context, Long timestamp) {
                                System.out.println("element = " + element);


                                String[] split = element.split(",");
//                                byte[] key = split[0].getBytes(StandardCharsets.UTF_8);
//                                byte[] value = element.getBytes(StandardCharsets.UTF_8);

                                UserDTO userDTO = new UserDTO();
                                userDTO.setName(split[0]);
                                userDTO.setAge(Integer.parseInt(split[1]));
                                userDTO.setBirthday(LocalDateTime.now());

//                                String s = split[0];

//                                return new ProducerRecord<>("test01", key, userDTO.toString().getBytes(StandardCharsets.UTF_8));
                                return new ProducerRecord<>("test01", userDTO.toString().getBytes(StandardCharsets.UTF_8));
                            }
                        }
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("atguigu-")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "")
                .build();


        sensorDS.sinkTo(kafkaSink);


        env.execute();
    }
}
