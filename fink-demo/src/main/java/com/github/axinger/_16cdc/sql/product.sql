CREATE TABLE `daily_statistics`
(
    `product_id`      varchar(20) NOT NULL,
    `total_quantity`  int         NOT NULL,
    `inventory_count` int         NOT NULL,
    `statistics_date` date        NOT NULL,
    `update_time`     timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`product_id`, `statistics_date`)
);

CREATE TABLE `inventory`
(
    `id`             int         NOT NULL AUTO_INCREMENT,
    `product_id`     varchar(20) NOT NULL,
    `quantity`       int         NOT NULL,
    `inventory_date` datetime    NOT NULL,
    PRIMARY KEY (`id`),
    KEY              `product_id` (`product_id`),
    CONSTRAINT `inventory_ibfk_1` FOREIGN KEY (`product_id`) REFERENCES `product` (`product_id`)
);


CREATE TABLE `product`
(
    `product_id`   varchar(20)  NOT NULL,
    `product_name` varchar(100) NOT NULL,
    PRIMARY KEY (`product_id`)
);
