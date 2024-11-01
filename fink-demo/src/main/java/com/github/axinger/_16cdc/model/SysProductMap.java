package com.github.axinger._16cdc.model;

import org.apache.flink.api.common.functions.MapFunction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class SysProductMap implements MapFunction<String, SysProduct> {
    @Override
    public SysProduct map(String value) throws Exception {
        String[] split = value.split(",");
        return SysProduct.builder()
                .id(Integer.valueOf(split[0]))
                .name(split[1])
                .quantity(Integer.valueOf(split[2]))
                .date(LocalDateTime.parse(split[3], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
                .build();
    }
}
