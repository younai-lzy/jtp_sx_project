CREATE DATABASE IF NOT EXISTS jtp_ads_warehouse LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_ads_warehouse';
USE jtp_ads_warehouse;

DROP TABLE IF EXISTS ods_ads_log_inc;
CREATE TABLE IF NOT EXISTS ods_ads_log_inc
(
    time_local STRING COMMENT '本地日期时间',
    requert_method STRING COMMENT '请求方式，比如GET、POST',
    requert_uri STRING COMMENT '请求地址',
    `status` STRING COMMENT '请求状态码',
    server_addr STRING COMMENT '服务器地址'
) COMMENT '广告投放日志数据-ODS表'
    PARTITIONED BY (dt STRING COMMENT '日期')
    STORED AS TEXTFILE
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_ads_warehouse/ods_ads_log_inc'
;
-- 加载数据

LOAD DATA INPATH '/warehouse/ads_logs/2024-10-01' OVERWRITE INTO TABLE jtp_ads_warehouse.ods_ads_log_inc PARTITION (dt = '2024-10-01');
LOAD DATA INPATH '/warehouse/ads_logs/2024-10-02' OVERWRITE INTO TABLE jtp_ads_warehouse.ods_ads_log_inc PARTITION (dt = '2024-10-01');
LOAD DATA INPATH '/warehouse/ads_logs/2024-10-03' OVERWRITE INTO TABLE jtp_ads_warehouse.ods_ads_log_inc PARTITION (dt = '2024-10-01');
LOAD DATA INPATH '/warehouse/ads_logs/2024-10-04' OVERWRITE INTO TABLE jtp_ads_warehouse.ods_ads_log_inc PARTITION (dt = '2024-10-01');
LOAD DATA INPATH '/warehouse/ads_logs/2024-10-05' OVERWRITE INTO TABLE jtp_ads_warehouse.ods_ads_log_inc PARTITION (dt = '2024-10-01');

-- SHOW PARTITIONS
SHOW PARTITIONS jtp_ads_warehouse.ods_ads_log_inc;

-- SHOW TABLE DATA
SELECT *
FROM ods_ads_log_inc
WHERE dt = '2024-10-01'
LIMIT 10
;

