package com.axing.demo.flink.model;

import lombok.Data;

/**
 *
 * @author xing
 */
@Data
public class ResponseModel {
    private String database;
    private String table;
    private String type;
    private String data;
}