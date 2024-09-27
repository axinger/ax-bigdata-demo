//package com.axing;
//
//import cn.hutool.core.util.StrUtil;
//import com.alibaba.fastjson.JSONObject;
//import com.axing.bean.DbBo;
//import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.util.Collector;
//import org.apache.kafka.connect.data.Field;
//import org.apache.kafka.connect.data.Struct;
//import org.apache.kafka.connect.source.SourceRecord;
//
//import java.util.List;
//
//public class MyDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
//
//
//    /**
//     * 新增：SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={file=mysql-bin.000220, pos=16692, row=1, snapshot=true}} ConnectRecord{topic='mysql_binlog_source.test_hqh.mysql_cdc_person', kafkaPartition=null, key=Struct{id=2}, keySchema=Schema{mysql_binlog_source.test_hqh.mysql_cdc_person.Key:STRUCT}, value=Struct{after=Struct{id=2,name=JIM,sex=male},source=Struct{version=1.2.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=0,snapshot=true,db=test_hqh,table=mysql_cdc_person,server_id=0,file=mysql-bin.000220,pos=16692,row=0},op=c,ts_ms=1603357255749}, valueSchema=Schema{mysql_binlog_source.test_hqh.mysql_cdc_person.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
//     * 更新：SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={ts_sec=1603357705, file=mysql-bin.000220, pos=22964, row=1, server_id=1, event=2}} ConnectRecord{topic='mysql_binlog_source.test_hqh.mysql_cdc_person', kafkaPartition=null, key=Struct{id=8}, keySchema=Schema{mysql_binlog_source.test_hqh.mysql_cdc_person.Key:STRUCT}, value=Struct{before=Struct{id=8,name=TOM,sex=male},after=Struct{id=8,name=Lucy,sex=female},source=Struct{version=1.2.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1603357705000,db=test_hqh,table=mysql_cdc_person,server_id=1,file=mysql-bin.000220,pos=23109,row=0,thread=41},op=u,ts_ms=1603357705084}, valueSchema=Schema{mysql_binlog_source.test_hqh.mysql_cdc_person.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
//     * 删除：SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={ts_sec=1603357268, file=mysql-bin.000220, pos=18510, row=1, server_id=1, event=2}} ConnectRecord{topic='mysql_binlog_source.test_hqh.mysql_cdc_person', kafkaPartition=null, key=Struct{id=7}, keySchema=Schema{mysql_binlog_source.test_hqh.mysql_cdc_person.Key:STRUCT}, value=Struct{before=Struct{id=7,name=TOM,sex=male},source=Struct{version=1.2.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1603357268000,db=test_hqh,table=mysql_cdc_person,server_id=1,file=mysql-bin.000220,pos=18655,row=0,thread=41},op=d,ts_ms=1603357268728}, valueSchema=Schema{mysql_binlog_source.test_hqh.mysql_cdc_person.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
//     *
//     * @param sourceRecord sourceRecord
//     * @param collector    out
//     */
//    @Override
//    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
//        JSONObject resJson = new JSONObject();
//        try {
//
//
//            Struct valueStruct = (Struct) sourceRecord.value();
//            Struct afterStruct = valueStruct.getStruct("after");
//            Struct beforeStruct = valueStruct.getStruct("before");
//            // 注意：若valueStruct中只有after,则表明插入；若只有before，说明删除；若既有before，也有after，则代表更新
//            if (afterStruct != null && beforeStruct != null) {
//
//
//                // 修改
//                System.out.println("Updating >>>>>>>");
//            } else if (afterStruct != null) {
//
//
//                // 插入
//                System.out.println("Inserting >>>>>>>");
//                List<Field> fields = afterStruct.schema().fields();
//                String name;
//                Object value;
//                for (Field field : fields) {
//
//
//                    name = field.name();
//                    value = afterStruct.get(name);
//                    resJson.put(name, value);
//                }
//            } else if (beforeStruct != null) {
//
//
//                // 删除
//                System.out.println("Deleting >>>>>>>");
//            } else {
//
//
//                System.out.println("No this operation ...");
//            }
//        } catch (Exception e) {
//
//
//            System.out.println("Deserialize throws exception:");
//        }
//        collector.collect(resJson.toJSONString());
//    }
//
//    @Override
//    public TypeInformation<String> getProducedType() {
//        return BasicTypeInfo.STRING_TYPE_INFO;
////        return BasicTypeInfo.of(JSONObject.class);
//    }
//}
