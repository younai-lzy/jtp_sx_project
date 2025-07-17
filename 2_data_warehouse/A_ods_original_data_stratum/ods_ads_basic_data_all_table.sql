CREATE DATABASE IF NOT EXISTS jtp_ads_warehouse location 'hdfs://node101:8020/user/spark/warehouse/jtp_ads_warehouse';
USE jtp_ads_warehouse;

-- 建表
-- 广告信息表
DROP TABLE IF EXISTS ods_ads_info_full;
CREATE EXTERNAL TABLE IF NOT EXISTS `ods_ads_info_full`
(
    `id`           bigint COMMENT '广告ID',
    `product_id`   bigint comment '产品ID',
    `material_id`  bigint comment '物料ID',
    `group_id`     bigint comment '广告组ID',
    `ad_name`      string comment '广告名称',
    `materail_url` string comment '物流URL地址'

) COMMENT '广告信息表'
    partitioned by (dt string comment '日期分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_ads_warehouse/ods_ads_info_full'
;

-- 加载数据
LOAD DATA INPATH 'hdfs://node101:8020/warehouse/ads_basic/2024-10-01/ads' OVERWRITE INTO TABLE jtp_ads_warehouse.ods_ads_info_full PARTITION (dt = '2024-10-01');

-- SHOW PARTITIONS
SHOW PARTITIONS jtp_ads_warehouse.ods_ads_info_full;

SELECT *
FROM ods_ads_info_full;

-- 广告平台映射表
DROP TABLE IF EXISTS ods_ads_platform_full;
CREATE TABLE IF NOT EXISTS ods_ads_platform_full
(
    `id`          BIGINT COMMENT '主键ID',
    `ad_id`       BIGINT COMMENT '广告ID',
    `platform_id` BIGINT COMMENT '平台ID',
    `create_time` STRING COMMENT '广告开始时间',
    `cancel_time` STRING COMMENT '广告结束时间'
) COMMENT '广告平台映射表'
    PARTITIONED BY (dt STRING COMMENT '日期分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_ads_warehouse/ods_ads_platform_full'
;

-- 加载数据

LOAD DATA INPATH 'hdfs://node101:8020/warehouse/ads_basic/2024-10-01/ads_platform' OVERWRITE INTO TABLE jtp_ads_warehouse.ods_ads_platform_full PARTITION (dt = '2024-10-01');

-- show partitions
SHOW PARTITIONS jtp_ads_warehouse.ods_ads_platform_full;

-- select data
SELECT *
FROM ods_ads_platform_full;

-- 广告信息表
DROP TABLE IF EXISTS ods_platform_info_full;
CREATE TABLE IF NOT EXISTS ods_platform_info_full
(
    `id`                STRING COMMENT '',
    `platform`          STRING,
    `platform_alias_zh` STRING
) COMMENT '广告信息表'
    PARTITIONED BY (dt STRING COMMENT '日期分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_ads_warehouse/ods_platform_info_full'
;

-- LOAD DATA
LOAD DATA INPATH 'hdfs://node101:8020/warehouse/ads_basic/2024-10-01/platform_info' OVERWRITE INTO TABLE jtp_ads_warehouse.ods_platform_info_full PARTITION (dt = '2024-10-01');

-- select data
SELECT *
FROM ods_platform_info_full;

-- 产品表
DROP TABLE IF EXISTS ods_product;
CREATE TABLE IF NOT EXISTS ods_product
(
    `id` bigint COMMENT '产品ID',
    `name` string comment '产品名称',
    `price` DECIMAL(16, 2) COMMENT '产品价格'
) COMMENT '产品表'
    PARTITIONED BY (dt STRING COMMENT '日期分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_ads_warehouse/ods_product'
;
-- 加载数据
LOAD DATA INPATH 'hdfs://node101:8020/warehouse/ads_basic/2024-10-01/product' OVERWRITE INTO TABLE jtp_ads_warehouse.ods_product PARTITION (dt = '2024-10-01');

-- SHOW PARTITIONS
SHOW PARTITIONS jtp_ads_warehouse.ods_product;

SELECT *
FROM ods_product;






