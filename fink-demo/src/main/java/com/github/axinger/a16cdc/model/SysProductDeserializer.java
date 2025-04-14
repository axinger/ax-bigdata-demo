package com.github.axinger.a16cdc.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SysProductDeserializer implements DebeziumDeserializationSchema<SysProduct> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<SysProduct> collector) throws IOException {
        // Assuming the value is in JSON format
        String jsonValue = new String((byte[]) sourceRecord.value(), StandardCharsets.UTF_8);
        SysProduct sysProduct = objectMapper.readValue(jsonValue, SysProduct.class);

        collector.collect(sysProduct);
    }

    @Override
    public TypeInformation<SysProduct> getProducedType() {
        return TypeInformation.of(SysProduct.class);
    }
}
