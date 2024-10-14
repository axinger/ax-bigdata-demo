package com.github.axinger.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 水位传感器类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class WaterSensor {
    // flink 要求为public
    // 无参构造器
    public String id;
    public Long ts;
    public Integer vc;
}
