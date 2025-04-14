-- 创建 MySQL CDC 源表（捕获 yield_records 表变更）
CREATE TABLE yield_records (
                               id BIGINT,
                               product_id INT,
                               num INT,
                               production_date TIMESTAMP(3),
                               PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'connector' = 'mysql-cdc',
      'hostname' = 'hadoop102',
      'port' = '3306',
      'username' = 'root',
      'password' = '123456',
      'database-name' = 'ax_test10_cdc',
      'table-name' = 'yield_records'
      );
