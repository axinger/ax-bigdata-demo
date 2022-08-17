package com.axing.demo.flink.config;

import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @description:
 * @author: wwp
 * @date: 2022-06-23 15:48
 */
public class MyJsonSchema implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) {

        Struct value = (Struct) sourceRecord.value();
        Struct source = value.getStruct("source");

        // get db
        String db = source.getString("db");
        String table = source.getString("table");

        // get type, create and read convert to insert
        String type = Envelope.operationFor(sourceRecord).toString().toLowerCase();
        if ("create".equals(type) || "read".equals(type)) {
            type = "insert";
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("database", db);
        jsonObject.put("table", table);
        jsonObject.put("type", type);
        // get data
        Struct before = value.getStruct("before");
        Struct after = value.getStruct("after");
        JSONObject dataJson = new JSONObject();
        List<Field> fields;
        // delete get before else get after
        if ("delete".equals(type)) {
            fields = before.schema().fields();
        } else {
            fields = after.schema().fields();
        }
        for (Field field : fields) {
            String field_name = field.name();
            Object fieldValue;
            if ("delete".equals(type)) {
                fieldValue = before.get(field);
            } else {
                fieldValue = after.get(field);
            }

            dataJson.put(field_name, fieldValue);
        }

        jsonObject.put("data", JSONObject.toJSONString(dataJson));
        collector.collect(JSONObject.toJSONString(jsonObject));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}