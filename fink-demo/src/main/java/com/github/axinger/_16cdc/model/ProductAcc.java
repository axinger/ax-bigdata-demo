package com.github.axinger._16cdc.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

// 累加器类，用于存储总和与生产次数
    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public  class ProductAcc {
        public Integer maxId = 0;
        public String name;
        public LocalDateTime date;
        public int totalQuantity = 0;
        public int productionCount = 0;
    }
