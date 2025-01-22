package com.github.axinger.doris;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class FlinkConfigBean {

    private FlinkConfig flink;


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FlinkConfig {
        private KafkaSource kafkaSource;
        private DorisSink dorisSink;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class KafkaSource {
        private String topic;
        private String bootstrapServers;
        private String format;
        private String startupMode;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DorisSink {
        private String fenodes;
        private String tableIdentifier;
        private String username;
        private String password;
        private String labelPrefix;
        private Map<String, String> properties;
    }
}
