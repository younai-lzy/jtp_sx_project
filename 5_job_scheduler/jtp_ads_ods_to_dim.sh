#!/bin/bash

if [ -n "$1" ] ; then
  date=$1
else
  date=`date -d '-1 days' +%F`
fi

echo "开始导入 $date 的数据..."
DIM_ADS_PLATFORM_INFO_FULL_SQL="
USE jtp_ads_warehouse;
WITH
    t1 AS (
        SELECT
            id,
            ads_id,
            platform_id,
            create_time,
            cancel_time
        FROM
            jtp_ads_warehouse.ods_ads_platform_full
        WHERE
            dt = '${date}'
    )
    ,
    t2 AS (
        SELECT
            id,
            platform,
            platform_alias_zh
        FROM
            jtp_ads_warehouse.ods_ads_info_full
        WHERE
            dt = '${date}'
    )
    ,
    t3 AS (
        SELECT
            id,
            name,
            price
        FROM
            jtp_ads_warehouse.ods_product
        WHERE
            dt = '${date}'
    )
    ,
    t4 AS (
        SELECT
            id,
            ad_name,
            materail_url
        FROM
            jtp_ads_warehouse.ods_platform_info_full
        WHERE
            dt = '${date}'
    )
INSERT OVERWRITE TABLE jtp_ads_warehouse.dim_ads_platform_info_full PARTITION (dt = '${date}')
SELECT
    t1.id,
    t1.ads_id,
    t1.ads_name,
    t2.ads_group_id,
    t1.platform_id,
    t1.create_time,
    t1.cancel_time,
    t2.platform,
    t2.platform_alias_zh,
    t3.name,
    t3.price,
    t4.ads_name,
    t4.materail_url
FROM t1
LEFT JOIN t2 ON t1.ads_id = t2.id
LEFT JOIN t3 ON t2.product_id = t3.id
LEFT JOIN t4 ON t1.platform_id = t4.id
"
/opt/module/spark/bin/beeline -u jdbc:hive2://node101:10001 -n bwie -e "${DIM_ADS_PLATFORM_INFO_FULL_SQL}"
echo "数据导入完成..."