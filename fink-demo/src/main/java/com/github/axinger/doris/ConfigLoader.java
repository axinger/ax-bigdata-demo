package com.github.axinger.doris;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

@Slf4j
public class ConfigLoader {

    private static final Yaml yaml = new Yaml();

    public static <T> T loadConfigYaml(String filePath, Class<T> configClass) throws IOException {
        try (InputStream inputStream = ConfigLoader.class.getClassLoader().getResourceAsStream(filePath)) {
            if (inputStream == null) {
                throw new IOException("找不到文件: " + filePath);
            }
            return yaml.loadAs(inputStream, configClass);
        }
    }

    public static String loadConfigSql(String filePath) throws IOException {
        try (InputStream inputStream = ConfigLoader.class.getClassLoader().getResourceAsStream(filePath)) {
            if (inputStream == null) {
                throw new IOException("找不到文件: " + filePath);
            }
            String sqlContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            log.info("sql语句={}", sqlContent);
            return sqlContent;
        }
    }
}
