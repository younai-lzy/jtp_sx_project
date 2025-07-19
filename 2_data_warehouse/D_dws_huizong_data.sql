/**
  对DWD层事实表：广告投放日志事务事实表，按照公共纬度进行初步汇总
  1.粒度：多个纬度组合
    ad_id 广告ID
    platform_id 投入平台ID
    client_province 省份
    client_os_type 操作系统类型
    client_browser_type 浏览器类型
  2.指标
    点击量
    曝光量
  3.时间
    每日每小时数据
 */
USE jtp_ads_warehouse;

--创建汇总表
DROP TABLE IF EXISTS jtp_ads_warehouse.dws_hour_common_ads_agg;
CREATE TABLE IF NOT EXISTS jtp_ads_warehouse.dws_hour_common_ads_agg(
    hr STRING COMMENT '小时',
    ad_id STRING COMMENT '广告ID',
    ads_name STRING COMMENT '广告名称',
    platform_id STRING COMMENT '推广平台ID',
    platform_name_zh STRING COMMENT '推广平台名称中文',
    client_province STRING COMMENT '省份',
    client_city STRING COMMENT '城市',
    client_os_type STRING COMMENT '操作系统类型',
    client_browser_type STRING COMMENT '浏览器类型',
    is_invalid_traffic STRING COMMENT '是否是异常流量',
    click_count BIGINT COMMENT '点击次数',
    impression_count BIGINT COMMENT '曝光次数'
)COMMENT '公共粒度汇总广告投放日志数据'
    PARTITIONED BY (dt STRING COMMENT '日期分区')
    STORED AS ORC
    TBLPROPERTIES ('orc.compression'='snappy')
    LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_ads_warehouse/dws_hour_common_ads_agg'
;

--TODO 对DWD层事实表进行汇总
INSERT OVERWRITE TABLE jtp_ads_warehouse.dws_hour_common_ads_agg PARTITION (dt='2024-10-01')
SELECT
    hour(from_unixtime(event_time/1000)) AS hr
     ,ads_id
     ,ads_name
     ,platform_id
     ,platform_name_zh
     ,client_province
     ,client_city
     ,client_os_type
     ,client_browser_type
     ,is_invalid_traffic
     ,count(if(event_type='click',ads_id,NULL))AS click_count
     ,count(ads_id) AS impression_count
FROM jtp_ads_warehouse.dwd_ads_event_log_inc
WHERE dt='2024-10-01'
  AND event_type IN ('click','impression')
GROUP BY hour(from_unixtime(event_time/1000))
       ,ads_id
       ,ads_name
       ,platform_id
       ,platform_name_zh
       ,client_province
       ,client_city
       ,client_os_type
       ,client_browser_type
       ,is_invalid_traffic
;

SHOW PARTITIONS jtp_ads_warehouse.dws_hour_common_ads_agg;

SELECT * FROM jtp_ads_warehouse.dws_hour_common_ads_agg;
