-- 创建 JDBC Sink 表（写入 yield_sum 结果）
CREATE TABLE yield_sum (
    product_id STRING,
    sum_num INT,
    sum_count INT,
    records_date DATE,
    update_time TIMESTAMP(3),
    PRIMARY KEY (product_id, records_date) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://hadoop102:3306/ax_test10_cdc',
    'table-name' = 'yield_sum',
    'username' = 'root',
    'password' = '123456'
);
