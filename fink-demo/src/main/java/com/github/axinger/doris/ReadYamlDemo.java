package com.github.axinger.doris;

import java.io.IOException;

public class ReadYamlDemo {
    public static void main(String[] args) throws IOException {

        ConfigBean configBean = ConfigLoader.loadConfig("application.yaml", ConfigBean.class);
        System.out.println("flinkConfigBean = " + configBean);

        String topic = configBean.getKafka().getTopic();
        System.out.println("topic = " + topic);
    }
}

