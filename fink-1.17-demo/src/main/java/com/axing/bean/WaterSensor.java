package com.axing.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 水位传感器类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}
