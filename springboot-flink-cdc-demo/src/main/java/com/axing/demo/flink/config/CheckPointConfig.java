package com.axing.demo.flink.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author xing
 */
@Configuration
@ConfigurationProperties(prefix = "flink")
@Data
public class CheckPointConfig {
    private String checkpointStorage;
    private String last;


    public String fileStorage() {
        return "file://" + checkpointStorage;
    }

    public String fullPath() {
        return fileStorage() + last;
    }


}
