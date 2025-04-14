-- 流式聚合计算并写入结果
INSERT INTO yield_sum
SELECT
    CAST(product_id AS STRING) AS product_id, -- 类型转换
    SUM(num) AS sum_num,
     CAST(COUNT(*) AS INT) AS sum_count,
    CAST(production_date AS DATE) AS records_date, -- 转日期
    CURRENT_TIMESTAMP AS update_time
FROM yield_records
WHERE
    product_id IS NOT NULL      -- 过滤空 product_id
  AND production_date IS NOT NULL -- 过滤空日期
GROUP BY
    product_id,
    CAST(production_date AS DATE); -- 按产品和日期聚合
