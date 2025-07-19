#!/bin/bash
if [ -n "$1" ] ; then
  date=$1
else
  date=`date -d '-1 days' +%F`
fi
echo "开始导入 $date 的数据..."

# 广告信息表
sqoop import \
--connect jdbc:mysql://node101:3306/jtp_ads \
--username root \
--password 123456 \
--query 'SELECT id, product_id, material_id, group_id, ad_name, materail_url FROM `jtp_ads`.`ads`  WHERE $CONDITIONS AND 1 = 1;' \
--delete-target-dir \
--target-dir "hdfs://node101:8020/warehouse/jtp_ads/${date}/ads/" \
--as-textfile \
--fields-terminated-by '\001' \
--null-string '\\N' \
--null-non-string '\\N' \
--num-mappers 1

# 广告平台信息表
sqoop import \
--connect jdbc:mysql://node101:3306/jtp_ads \
--username root \
--password 123456 \
--query 'SELECT id, ad_id, platform_id, create_time, cancel_time FROM `jtp_ads`.`ads_platform`  WHERE $CONDITIONS AND 1 = 1;' \
--delete-target-dir \
--target-dir "hdfs://node101:8020/warehouse/jtp_ads/${date}/ads_platform/" \
--as-textfile \
--fields-terminated-by '\001' \
--null-string '\\N' \
--null-non-string '\\N' \
--num-mappers 1

sqoop import \
--connect jdbc:mysql://node101:3306/jtp_ads \
--username root \
--password 123456 \
--query 'SELECT id, platform, platform_alias_zh FROM `jtp_ads`.`platform_info`  WHERE $CONDITIONS AND 1 = 1;' \
--delete-target-dir \
--target-dir "hdfs://node101:8020/warehouse/jtp_ads/${date}/platform_info/" \
--as-textfile \
--fields-terminated-by '\001' \
--null-string '\\N' \
--null-non-string '\\N' \
--num-mappers 1

sqoop import \
--connect jdbc:mysql://node101:3306/jtp_ads \
--username root \
--password 123456 \
--query 'SELECT id, name, price FROM `jtp_ads`.`product`  WHERE $CONDITIONS AND 1 = 1;' \
--delete-target-dir \
--target-dir "hdfs://node101:8020/warehouse/jtp_ads/${date}/product/" \
--as-textfile \
--fields-terminated-by '\001' \
--null-string '\\N' \
--null-non-string '\\N' \
--num-mappers 1