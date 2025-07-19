CREATE DATABASE IF NOT EXISTS jtp_ads_warehouse LOCATION 'hdfs://node101:8020/user/spark/warehouse/jtp_ads_warehouse'
USE jtp_ads_warehouse;

/*
-- 对原始广告投放日志数据，进行etl处理，步骤整体如下：
    step1.提取字段值，初步解析
        request_uri -> 分割，解析，提取等
    step2.关联维度字段
        补充维度字段
    step3.解析IP地址
        获取地理区域信息，使用UDF函数
    step4.解析客户端信息
        获取客户端操作系统数据，使用UDF函数
    step5.判断流量是否异常
        比如1秒钟，同一个设备访问10次，属于异常流量，进行标识
 */

SELECT kv_map['t']         AS event_time
     , url_array[3]        AS event_type
     , kv_map['id']        AS ads_id
     , url_array[2]        AS platform_name_en
     , kv_map['device_id'] AS client_ip
     , kv_map['ip']        AS client_user_agent
     , kv_map['os_type']   AS os_type
FROM (SELECT requert_uri
           -- 字符串分裂
           , split(split(requert_uri, '\\?')[0], '/')           AS url_array
           -- 转换集合
           , str_to_map(split(requert_uri, '\\?')[1], '&', '=') AS kv_map
      FROM ods_ads_log_inc
      WHERE dt = '2024-10-01') t1
;

-- todo parse_url() 和 reflect()
DESC FUNCTION parse_url;
/*
 parse_url专门针对url请求地址进行解析函数
 第一个参数：域名部分：http://www.baidu.com
 第二个参数：PATH路径部分：/ad/tencent/impression url?之前内容
 第三个参数：QUERY查询部分：id=129&t=172774928
 */

-- 反射函数
DROP TABLE IF EXISTS tmp_dwd_ads_event_log_parse;
CREATE TABLE IF NOT EXISTS tmp_dwd_ads_event_log_parse
AS
SELECT parse_url('https://www.baidu.com' || requert_uri, 'QUERY', 't')          AS event_time,
       split(parse_url('https://www.baidu.com' || requert_uri, 'PATH'), '/')[3] AS event_type,
       parse_url('https://www.baidu.com' || requert_uri, 'QUERY', 'id')         AS ads_id,
       split(parse_url('https://www.baidu.com' || requert_uri, 'PATH'), '/')[2] AS platform_name_en,
       parse_url('https://www.baidu.com' || requert_uri, 'QUERY', 'ip')         AS client_ip,
       parse_url('https://www.baidu.com' || requert_uri, 'QUERY', 'device_id')  AS client_device_id,
       parse_url('https://www.baidu.com' || requert_uri, 'QUERY', 'os_type')    AS client_os_type,
       reflect('java.net.URLDecoder', 'decode', parse_url('https://www.baidu.com' || requert_uri, 'QUERY', 'ua'),
               'utf-8')                                                         AS client_user_agent
FROM ods_ads_log_inc
WHERE dt = '2024-10-01'
;

SELECT *
FROM tmp_dwd_ads_event_log_parse
;

-- STEP2.关联维度数据
-- 补充维度字段值
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
                    WHERE dt = '2024-10-01') t2 ON t1.ads_id = t2.ads_id
;

SELECT *
FROM tmp_dwd_ads_event_log_dim;


-- TODO 3.解析IP地址
-- 获取地理区域信息， 使用UDF函数
DROP TABLE IF EXISTS ;

REFRESH jtp_ads_warehouse;
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
     -- {'area':'0', 'city':'0', 'country':'中国', 'isp':'腾讯','province':'0'}
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

SELECT *
FROM tmp_dwd_ads_event_log_region;

SHOW FUNCTIONS LIKE 'ip_to_region';
-- 创建udf函数
-- CREATE FUNCTION default.ua_to_browser AS 'com.sina.UaToBrowser'
--  USING JAR 'hdfs://node101:8020/warehouse/ads_jars/jtp-amazon-warehouse-1.0-SNAPSHOT.jar';

-- 解析客户端信息
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

-- 查询数据
SELECT *
FROM tmp_dwd_ads_event_log_browser;

/*
 异常流量表示判断规则：
    1.同一IP访问过快：同一个IP地址，在5分钟（5 * 60 * 1000）内超过100次
    2.同一IP固定周期访问：同一个IP地址，固定周期访问超过5次
    3.同一设备过快
    4.同一设备固定周期访问
 */

--  todo 1.同一IP访问过快：同一个IP地址，在5分钟（5 * 60 * 1000）内超过100次
SELECT
    DISTINCT client_ip
FROM
    (
        SELECT client_ip
             , ads_id
             , event_time
             -- 使用count聚合开窗函数
             , count(1) OVER (PARTITION BY client_ip, ads_id ORDER BY CAST(event_time AS BIGINT)
            RANGE BETWEEN 300000 PRECEDING AND CURRENT ROW
            ) AS cnt
        FROM tmp_dwd_ads_event_log_parse
    ) t1
WHERE t1.cnt > 100
;

-- todo 2.同一ip固定周期访问：同一个IP地址，固定周期访问超过5次
-- 若同一Ip对同一广告有周期性的访问记录（例如每隔10s, 访问一次），则认定该ip的所有流量均为异常流量

-- s3 按照ip、ads和interval分组计数并过滤
SELECT
    DISTINCT
    client_ip, ads_id, interval_ms, count(1) AS cnt
FROM
    (
        SELECT
            client_ip, ads_id, event_time, next_event_time
             -- s2 计算访问时间间隔
             , (next_event_time - event_time) AS interval_ms
        FROM
            (
                -- s1 获取下一次访问时间
                SELECT
                    client_ip, ads_id, event_time
                     , lead(event_time, 1, 0) over (PARTITION BY client_ip, ads_id ORDER BY event_time) AS next_event_time
                FROM tmp_dwd_ads_event_log_parse
            )t1
    )t2
GROUP BY client_ip, ads_id, interval_ms
HAVING count(1) > 5
;

-- TODO 3 同一设备访问过快：5分钟内超过100次
-- 若同一设备在短时间内访问（包括曝光和点击）同一广告多次，则认定该设备的所有流量均为异常流量
SELECT
    DISTINCT client_device_id
FROM
    (
        SELECT
            client_device_id, ads_id, event_time
            -- 使用count聚合开窗函数
            , count(1) OVER (PARTITION BY client_device_id, ads_id ORDER BY cast(event_time AS BIGINT)
                RANGE BETWEEN 300000 PRECEDING AND CURRENT ROW
            ) AS cnt
        FROM tmp_dwd_ads_event_log_parse
    ) t1
WHERE t1.cnt > 100
;

-- todo 4.同一设备固定周期访问：固定周期访问超过5次
-- 若同一设备对同一广告有周期性的访问记录（例如每隔10s，访问一次），则认定该设备的所有流量均为异常流量
-- s3.按照device, ads_id, interval_ms, count(1) AS cnt
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
                     , lead() over (PARTITION BY client_device_id, ads_id ORDER BY event_time) AS next_event_time
                FROM tmp_dwd_ads_event_log_parse
            )t1
    )t2
GROUP BY client_device_id, ads_id, interval_ms
HAVING count(1) > 5
;

--
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

SELECT *
FROM tmp_dwd_ads_event_log_traffic
;



