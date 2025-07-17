CREATE DATABASE IF NOT EXISTS jtp_ads_warehouse LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_ads_warehouse'
USE jtp_ads_warehouse;

-- 广告事件日志事实表
DROP TABLE IF EXISTS dwd_ads_event_log_inc;
CREATE TABLE IF NOT EXISTS dwd_ads_event_log_inc
(
    event_time BIGINT COMMENT '事件时间',
    event_type STRING COMMENT '事件类型',
    ads_id STRING COMMENT '广告ID',
    -- 维度数据字段
    ads_name STRING COMMENT '广告名称',
    ads_product_id STRING COMMENT '广告商品ID',
    ads_product_name STRING COMMENT '广告商品名称',
    ads_product_price DECIMAL(16, 2) COMMENT '广告商品价格',
    ads_material_id STRING COMMENT '广告素材id',
    ads_group_id STRING COMMENT '广告组ID',
    platform_id STRING COMMENT '推广平台ID',
    platform_name_en STRING COMMENT '推广平台名称（英文）',
    platform_name_zh STRING COMMENT '推广平台名称（中文）',

    -- 地理区域信息
    client_country STRING COMMENT '客户端所处国家',
    client_area STRING COMMENT '客户端所处地区',
    client_province STRING COMMENT '客户端所处省份',
    client_city STRING COMMENT '客户端所处城市',
    client_ip STRING COMMENT '客户端ip地址',
    client_device_id STRING COMMENT '客户端设备id',
    client_os_type STRING COMMENT '客户端操作系统类型',
    -- 解析字段获取
    client_os_version STRING COMMENT '客户端操作系统版本',
    client_browser_type STRING COMMENT '客户端浏览器类型',
    client_browser_version STRING COMMENT '客户端浏览器版本',
    client_user_agent STRING COMMENT '客户端UA',
    is_invalid_traffic BOOLEAN COMMENT '是否是异常流量'

) COMMENT '广告事件日志事实表'
    PARTITIONED BY (dt STRING COMMENT '日期分区')
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'snappy')
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_ads_warehouse/dwd_ads_event_log_inc'
;


INSERT OVERWRITE TABLE dwd_ads_event_log_inc PARTITION (dt = '2024-10-01')
SELECT
    CAST(event_time AS BIGINT)
    , event_type
    , ads_id
    , ads_name
    , ads_product_id
    , ads_product_name
    , ads_product_price
    , ads_materail_id
    ,    ads_group_id
    ,    platform_id
    ,    platform_name_en
    ,    platform_name_zh

    -- 地理区域信息
    ,    client_country
    ,    client_area
    ,    client_province
    ,    client_city
    ,    client_ip
    ,    client_device_id
    ,    client_os_type
    -- 解析字段获取
    ,    client_os_version
    ,    client_browser_type
    ,    client_browser_version
    ,    client_user_agent
    ,    is_invalid_traffic
FROM jtp_ads_warehouse.tmp_dwd_ads_event_log_traffic
;

SHOW PARTITIONS jtp_ads_warehouse.dwd_ads_event_log_inc;

SELECT *
FROM dwd_ads_event_log_inc
WHERE dt = '2024-10-01'
AND is_invalid_traffic = true;