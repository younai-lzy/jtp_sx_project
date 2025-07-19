#!/bin/bash
if [ -n "$1" ]; then
  date=$1
else
  date=$(date -d '-1 days' +%F)
fi

echo "开始导入 $date 的数据..."
#提取字段值初步解析
TMP_DWD_ADS_EVENT_LOG_PARSE_SQL="
USE jtp_ads_warehouse;
DROP TABLE IF EXISTS jtp_ads_warehouse.tmp_dwd_ads_event_log_parse
AS
SELECT
    parse_url('https://www.baidu.com' || requert_uri, 'QUERY', 't')          AS event_time,
           split(parse_url('https://www.baidu.com' || requert_uri, 'PATH'), '/')[3] AS event_type,
           parse_url('https://www.baidu.com' || requert_uri, 'QUERY', 'id')         AS ads_id,
           split(parse_url('https://www.baidu.com' || requert_uri, 'PATH'), '/')[2] AS platform_name_en,
           parse_url('https://www.baidu.com' || requert_uri, 'QUERY', 'ip')         AS client_ip,
           parse_url('https://www.baidu.com' || requert_uri, 'QUERY', 'device_id')  AS client_device_id,
           parse_url('https://www.baidu.com' || requert_uri, 'QUERY', 'os_type')    AS client_os_type,
           reflect('java.net.URLDecoder', 'decode', parse_url('https://www.baidu.com' || requert_uri, 'QUERY', 'ua'),
                   'utf-8')                                                         AS client_user_agent
    FROM ods_ads_log_inc
    WHERE dt = '${date}'
;

"
# 关联维度数据
TMP_DWD_ADS_EVENT_LOG_DIM_SQL="
USE jtp_ads_warehouse;
DROP TABLE IF EXISTS tmp_dwd_ads_event_log_dim;
CREATE TABLE IF NOT EXISTS tmp_dwd_ads_event_log_dim
AS
SELECT t1.event_time
     , t1.event_type
     , t1.ads_id
     , t2.ads_name
     , t2.product_id    AS ads_product_id
     , t2.product_name  AS ads_product_name
     , t2.product_price AS ads_product_price
     , t2.materail_id   AS ads_materail_id
     , t2.ads_group_id  AS ads_group_id
     , t2.platform_id
     , t1.platform_name_en
     , t2.platform_name_zh
     , t1.client_ip
     , t1.client_device_id
     , t1.client_os_type
     , t1.client_user_agent
FROM tmp_dwd_ads_event_log_parse t1
         LEFT JOIN (SELECT id,
                           ads_id,
                           ads_name,
                           ads_group_id,
                           product_id,
                           product_name,
                           product_price,
                           materail_id,
                           materail_url,
                           platform_id,
                           platform_name,
                           platform_name_zh
                    FROM jtp_ads_warehouse.dim_ads_platform_info_full
                    WHERE dt = '${date}') t2 ON t1.ads_id = t2.ads_id
;

"
#3.解析ip地址
TMP_DWD_ADS_EVENT_LOG_REGION_SQL="
USE jtp_ads_warehouse;
DROP TABLE IF EXISTS tmp_dwd_event_log_region;
CREATE TABLE IF NOT EXISTS tmp_dwd_ads_event_log_region
AS
SELECT event_time
     , event_type
     , ads_id
     , ads_name
     , ads_product_id
     , ads_product_name
     , ads_product_price
     , ads_materail_id
     , ads_group_id
     , platform_id
     , platform_name_en
     , platform_name_zh
     , region_map['country'] AS client_country
     , region_map['area'] AS client_area
     , region_map['province'] AS client_province
     , region_map['city'] AS client_city
     , client_ip
     , client_device_id
     , client_os_type
     , client_user_agent
FROM (SELECT *, default.ip_to_region(client_ip) AS region_map
      FROM tmp_dwd_ads_event_log_dim) t1
;
"
# 4.解析客户端信息
TMP_DWD_ADS_EVENT_LOG_BROWSER_SQL="
USE jtp_ads_warehouse;
DROP TABLE IF EXISTS tmp_dwd_ads_event_log_browser;
CREATE TABLE IF NOT EXISTS tmp_dwd_ads_event_log_browser
AS
SELECT event_time,
       event_type,
       ads_id,
       ads_name,
       ads_product_id,
       ads_product_name,
       ads_product_price,
       ads_materail_id,
       ads_group_id,
       platform_id,
       platform_name_en,
       platform_name_zh,
       client_country,
       client_area,
       client_province,
       client_city,
       client_ip,
       client_device_id,
       client_os_type,
       browser_map['os_version'] AS client_os_version,
       browser_map['browser'] AS client_browser_type,
       browser_map['browser_version'] AS client_browser_version,
       client_user_agent
FROM (SELECT *,
             default.ua_to_browser(client_user_agent) AS browser_map
      FROM tmp_dwd_ads_event_log_region)
;
"

# 5.判断流浪是否异常
TMP_DWD_ADS_EVENT_LOG_TRAFFIC_SQL="
USE jtp_ads_warehouse;
DROP TABLE IF EXISTS tmp_dwd_ads_event_log_traffic;
CREATE TABLE IF NOT EXISTS tmp_dwd_ads_event_log_traffic
AS
WITH
    -- 规则：ip
    tmp_ip AS (
        SELECT
            DISTINCT client_ip
        FROM
            (
                SELECT
                    client_ip, ads_id, event_time
                     -- 使用count聚合开窗函数
                     , count(1) OVER(PARTITION BY client_ip, ads_id ORDER BY cast(event_time AS BIGINT)
                         RANGE BETWEEN 300000 PRECEDING AND CURRENT ROW ) AS cnt
                FROM tmp_dwd_ads_event_log_parse
            )t1
        WHERE t1.cnt > 100
        UNION
        SELECT
            DISTINCT client_ip
        FROM
            (
                -- 2.计算相邻2次访问时间间隔
                SELECT
                    client_ip, ads_id, event_time, next_event_time
                     -- 计算访问时间间隔
                     , (next_event_time - event_time) AS interval_ms
                FROM
                    (-- s1 获取下一次访问时间
                        SELECT
                            client_ip, ads_id, event_time
                             , lead(event_time, 1, 0) over (PARTITION BY client_ip, ads_id ORDER BY event_time) AS next_event_time
                        FROM tmp_dwd_ads_event_log_parse
                    )t1
            )t2
        GROUP BY client_ip, ads_id, interval_ms
        HAVING count(1) > 5
    ),
    --规则：device
    tmp_device AS (
        SELECT
            DISTINCT client_device_id
        FROM
            (
                SELECT
                    client_device_id, ads_id, event_time
                     -- 使用count聚合开窗函数
                     , count(1) OVER(PARTITION BY client_device_id, ads_id ORDER BY cast(event_time AS BIGINT)
                    RANGE BETWEEN 300000 PRECEDING AND CURRENT ROW ) AS cnt
                FROM tmp_dwd_ads_event_log_parse
            )t1
        WHERE t1.cnt > 100
        UNION
        SELECT
            DISTINCT client_device_id
        FROM
            (
                -- 2.计算相邻2次访问时间间隔
                SELECT
                    client_device_id, ads_id, event_time, next_event_time
                     -- 计算访问时间间隔
                     , (next_event_time - event_time) AS interval_ms
                FROM
                    (-- s1 获取下一次访问时间
                        SELECT
                            client_device_id, ads_id, event_time
                             , lead(event_time, 1, 0) over (PARTITION BY client_device_id, ads_id ORDER BY event_time) AS next_event_time
                        FROM tmp_dwd_ads_event_log_parse
                    )t1
            )t2
        GROUP BY client_device_id, ads_id, interval_ms
        HAVING count(1) > 5
    )
SELECT
    t1.event_time
    , t1.event_type
    , t1.ads_id
    , t1.ads_name
    , t1.ads_product_id
    , t1.ads_product_name
    , t1.ads_product_price
    , t1.ads_materail_id
    , t1.ads_group_id
    , t1.platform_id
    , t1.platform_name_en
    , t1.platform_name_zh
    , t1.client_country
    , t1.client_area
    , t1.client_province
    , t1.client_city
    , t1.client_ip
    , t1.client_device_id
    , t1.client_os_type
    , t1.client_os_version
    , t1.client_browser_type
    , t1.client_browser_version
    , t1.client_user_agent
    -- 判断确定是否为异常流量
    , t2.client_ip IS NULL AND t3.client_device_id IS NULL AS is_invalid_traffic
FROM tmp_dwd_ads_event_log_browser t1
LEFT JOIN tmp_ip t2 ON t1.client_ip = t2.client_ip
LEFT JOIN tmp_device t3 ON t1.client_device_id = t3.client_device_id
;
"

# 查询插入
DWD_ADS_EVENT_LOG_INC_SQL="
USE jtp_ads_warehouse;
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
    ,    client_country
    ,    client_area
    ,    client_province
    ,    client_city
    ,    client_ip
    ,    client_device_id
    ,    client_os_type
    ,    client_os_version
    ,    client_browser_type
    ,    client_browser_version
    ,    client_user_agent
    ,    is_invalid_traffic
FROM jtp_ads_warehouse.tmp_dwd_ads_event_log_traffic
;

"
/opt/module/spark/bin/beeline -u jdbc:hive2://node101:10001 -n bwie -e "${TMP_DWD_ADS_EVENT_LOG_PARSE_SQL}${TMP_DWD_ADS_EVENT_LOG_DIM_SQL}
${TMP_DWD_ADS_EVENT_LOG_REGION_SQL}${TMP_DWD_ADS_EVENT_LOG_BROWSER_SQL}${TMP_DWD_ADS_EVENT_LOG_TRAFFIC_SQL}${DWD_ADS_EVENT_LOG_INC_SQL}"
