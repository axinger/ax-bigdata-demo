package com.github.axinger.doris;

import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;

public class ConfigLoader {

    private static final Yaml yaml = new Yaml();

    public static <T> T loadConfig(String filePath, Class<T> configClass) throws IOException {
        try (InputStream inputStream = ConfigLoader.class.getClassLoader().getResourceAsStream(filePath)) {
            if (inputStream == null) {
                throw new IOException("找不到文件: " + filePath);
            }
            return yaml.loadAs(inputStream, configClass);
        }
    }
}
