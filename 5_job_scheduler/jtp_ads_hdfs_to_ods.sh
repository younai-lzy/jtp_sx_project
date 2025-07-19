#!/bin/bash

if [ -n "$1" ] ; then
  date=$1
else
  date=`date -d '-1 days' +%F`
fi

# 加载数据到表中
ADS_LOG_LOAD_ODS_SQL="load data inpath '/warehouse/ads_logs/${date}' overwrite into table jtp_ads_warehouse.ods_ads_info_full partition (dt = '${date}');
"
# 广告信息表
ODS_ADS_INFO_FULL_SQL="load data inpath '/warehouse/ads_basic/${date}/ads' overwrite into table jtp_ads_warehouse.ods_ads_info_full partition (dt = '${date}');
"
# 广告平台映射表
ODS_ADS_PLATFORM_FULL_SQL="load data inpath '/warehouse/ads_basic/${date}/ads_platform' overwrite into table jtp_ads_warehouse.ods_ads_platform_full partition (dt = '${date}');
"

# 广告平台信息表
ODS_PLATFORM_INFO_FULL_SQL="load data inpath '/warehouse/ads_basic/${date}/platform_info' overwrite into table jtp_ads_warehouse.ods_platform_info_full partition (dt = '${date}');
"

# 产品信息表
ODS_PRODUCT_FULL_SQL="load data inpath '/warehouse/ads_basic/${date}/product' overwrite into table jtp_ads_warehouse.ods_product partition (dt = '${date}');
"

/opt/module/spark/bin/beeline -u jdbc:hive2://node101:10001 -n bwie -e "${ADS_LOG_LOAD_ODS_SQL}${ODS_ADS_INFO_FULL_SQL}${ODS_ADS_PLATFORM_FULL_SQL}${ODS_PLATFORM_INFO_FULL_SQL}${ODS_PRODUCT_FULL_SQL}"
echo "数据导入完成..."