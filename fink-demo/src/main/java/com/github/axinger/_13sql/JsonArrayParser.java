package com.github.axinger._13sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.functions.TableFunction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonArrayParser extends TableFunction<Tuple3<Long, String, Integer>> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public void eval(String jsonArray) {
        if (jsonArray == null || jsonArray.trim().isEmpty()) {
            return;
        }
        try {
            JsonNode arrayNode = objectMapper.readTree(jsonArray);
            if (!arrayNode.isArray()) {
                System.err.println("Expected JSON array but got: " + arrayNode.getNodeType());
                return;
            }
            for (JsonNode node : arrayNode) {
                if (!node.has("id") || !node.has("name") || !node.has("age")) {
                    System.err.println("Missing required fields in JSON object: " + node);
                    continue;
                }
                long id = node.get("id").asLong();
                String name = node.get("name").asText();
                int age = node.get("age").asInt();
                collect(new Tuple3<>(id, name, age));
            }
        } catch (Exception e) {
            System.err.println("Failed to parse JSON array: " + jsonArray);
            e.printStackTrace();
        }
    }

    @Override
    public TypeInformation<Tuple3<Long, String, Integer>> getResultType() {
        return new TupleTypeInfo<>(
                TypeInformation.of(Long.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Integer.class)
        );
    }
}
