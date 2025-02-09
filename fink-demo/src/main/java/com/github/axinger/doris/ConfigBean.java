package com.github.axinger.doris;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConfigBean {

    private KafkaConfigBean kafka;
    private DorisConfigBean doris;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class KafkaConfigBean {
        private String topic;
        private String bootstrapServers;
        private String format;
        private String startupMode;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DorisConfigBean {
        private String fenodes;
        private String tableIdentifier;
        private String username;
        private String password;
        private String labelPrefix;
        private Map<String, String> properties;
    }
}
