CREATE TABLE `yield_records` (
                                 `id` bigint NOT NULL COMMENT '自增主键',
                                 `product_id` int DEFAULT NULL COMMENT '产品编号',
                                 `num` int DEFAULT NULL COMMENT '入库数量',
                                 `production_date` datetime DEFAULT NULL COMMENT '入库日期',
                                 PRIMARY KEY (`id`)
);

CREATE TABLE `yield_sum` (
                             `product_id` varchar(20) NOT NULL COMMENT '产品编号',
                             `sum_num` int NOT NULL COMMENT '每日入库总数量',
                             `sum_count` int NOT NULL COMMENT '每日入库次数',
                             `records_date` date NOT NULL COMMENT '入库日期',
                             `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                             PRIMARY KEY (`product_id`,`records_date`) USING BTREE
)
