package com.github.axinger._16cdc.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public  class SysProduct {

        // 数据id
        private Integer id;
        // 产品名称
        public String name;

        // 生产的数量,统计每个产品每日生产总数
        private Integer quantity;

        // 生产时间时间
//    public long ts;
        public String date;
    }
