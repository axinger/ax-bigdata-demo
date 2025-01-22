package com.github.axinger.doris;

import java.io.IOException;

public class ReadYamlDemo {
    public static void main(String[] args) throws IOException {

        FlinkConfigBean flinkConfigBean = ConfigLoader.loadConfig("application.yaml", FlinkConfigBean.class);
        System.out.println("flinkConfigBean = " + flinkConfigBean);

        String topic = flinkConfigBean.getFlink().getKafkaSource().getTopic();
        System.out.println("topic = " + topic);

    }
}

