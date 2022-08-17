package com.axing.demo.flink.model;


import lombok.Data;
import lombok.Getter;

/**
 * @author xing
 */
public interface CdcType {

    String insert = "insert";

    String delete = "delete";
}
