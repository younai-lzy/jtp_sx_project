# 广告信息表
sqoop import \
--connect jdbc:mysql://node101:3306/jtp_ads \
--username root \
--password 123456 \
--query 'SELECT id, product_id, material_id, group_id, ad_name, materail_url FROM `jtp_ads`.`ads`  WHERE $CONDITIONS AND 1 = 1;' \
--delete-target-dir \
--target-dir 'hdfs://node101:8020/warehouse/dmp_ad/2025-06-20/ads/' \
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
--target-dir 'hdfs://node101:8020/warehouse/dmp_ad/2025-06-20/ads_platform/' \
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
--target-dir 'hdfs://node101:8020/warehouse/dmp_ad/2025-06-20/platform_info/' \
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
--target-dir 'hdfs://node101:8020/warehouse/dmp_ad/2025-06-20/product/' \
--as-textfile \
--fields-terminated-by '\001' \
--null-string '\\N' \
--null-non-string '\\N' \
--num-mappers 1