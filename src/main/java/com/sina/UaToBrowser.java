package com.sina;

import cn.hutool.http.useragent.UserAgent;
import cn.hutool.http.useragent.UserAgentUtil;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;
import java.util.Map;

public class UaToBrowser extends UDF {

  public Map<String,String> evaluate(String useragent) {
    //定义集合
    HashMap<String, String> map = new HashMap<>();

//    1.解析
    UserAgent ua = UserAgentUtil.parse(useragent);
    //2.解析
    map.put("browser", ua.getBrowser().toString());
    map.put("browser_version", ua.getVersion());
    map.put("engine", ua.getEngine().toString());
    map.put("engine_version", ua.getEngineVersion());
    map.put("os", ua.getOs().toString());
    map.put("os_version", ua.getOsVersion());
    map.put("platform", ua.getPlatform().toString());
    map.put("is_mobile", ua.isMobile() ? "1" : "0");

    return map;
  }
}
