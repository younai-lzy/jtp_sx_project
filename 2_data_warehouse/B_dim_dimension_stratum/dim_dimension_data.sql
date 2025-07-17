CREATE DATABASE IF NOT EXISTS jtp_ads_warehouse location 'hdfs://node101:8020/user/spark/warehouse/jtp_ads_warehouse';
USE jtp_ads_warehouse;
/*
构建维度表
以ads_platform（ods_ads_platform_full）为主表，关联其他表

 */
-- 维度表
DROP TABLE IF EXISTS dim_ads_platform_info_full;
CREATE TABLE IF NOT EXISTS dim_ads_platform_info_full
(
    id BIGINT COMMENT '标识主键',
    ads_id BIGINT COMMENT '广告ID',
    ads_name STRING COMMENT '广告名称',
    ads_group_id BIGINT COMMENT '广告组ID',
    product_id BIGINT COMMENT '产品ID',
    product_name STRING COMMENT '产品名称',
    product_price DECIMAL(16, 2) COMMENT '产品价格',
    materail_id BIGINT COMMENT '物料ID',
    materail_url STRING COMMENT '物料URL地址',
    platform_id BIGINT COMMENT '平台ID',
    platform_name STRING COMMENT '平台名称',
    platform_name_zh STRING COMMENT '平台中文名称',
    create_time STRING COMMENT '广告展示开始时间',
    cancel_time STRING COMMENT '广告展示取消时间'
) COMMENT '广告平台投放维度表'
    PARTITIONED BY (dt STRING COMMENT '日期分区')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_ads_warehouse/dim_ads_platform_info_full'
    TBLPROPERTIES ('orc.compression' = 'snappy')

;
-- 加载数据
WITH
    T1 AS (
        SELECT id, ad_id, platform_id, create_time, cancel_time, dt
        FROM jtp_ads_warehouse.ods_ads_platform_full
        WHERE dt = '2024-10-01'
    ),
    T2 AS (
        SELECT id, product_id, material_id, group_id, ad_name, materail_url, dt
        FROM jtp_ads_warehouse.ods_ads_info_full
        WHERE dt = '2024-10-01'
    ),
    T3 AS (
        SELECT id, name, price, dt
        FROM jtp_ads_warehouse.ods_product
        WHERE dt = '2024-10-01'
    ),
    T4 AS (
        SELECT id, platform, platform_alias_zh, dt
        FROM jtp_ads_warehouse.ods_platform_info_full
        WHERE dt = '2024-10-01'
    )
INSERT OVERWRITE TABLE dim_ads_platform_info_full PARTITION (dt = '2024-10-01')
SELECT
    T1.id
    , T1.ad_id
    , T2.ad_name, T2.group_id
    , T2.product_id
    , T3.name, T3.price
    , T2.material_id, T2.materail_url
    , T1.platform_id
    , T4.platform, T4.platform_alias_zh
    , T1.create_time, T1.cancel_time
FROM T1
LEFT JOIN T2 ON T1.ad_id = T2.id
LEFT JOIN T3 ON T2.product_id = T3.id
LEFT JOIN T4 ON T1.platform_id = T4.id
;
SELECT *
FROM dim_ads_platform_info_full;

