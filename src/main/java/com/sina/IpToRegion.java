package com.sina;


import org.apache.hadoop.hive.ql.exec.UDF;
import org.lionsoul.ip2region.DbConfig;
import org.lionsoul.ip2region.DbSearcher;

import java.util.HashMap;
import java.util.Map;

public class IpToRegion extends UDF {
  public Map<String, String> evaluate(String str) throws Exception {
    //定义集合
    Map<String, String> map = new HashMap<String,String>();

    //1.DBSearch对象
    DbSearcher dbSearcher = new DbSearcher(
      new DbConfig(), "/usr/local/ip2region/ip2region.db"
    );

//    2.查找
    String region = dbSearcher.binarySearch(str).getRegion();

    // 3. 解析
    String[] split = region.split("\\|");
    map.put("country", split[0]);
    map.put("area", split[1]);
    map.put("province", split[2]);
    map.put("city", split[3]);
    map.put("isp", split[4]);

    return map;

  }

}
