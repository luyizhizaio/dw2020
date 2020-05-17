package com.kyrie.dw.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;

public class LogUtils {

  //启动日志校验
  public static boolean validateStart(String log) {

    //{}

    if(!log.startsWith("{") || !log.endsWith("}")){
      return false;
    }
    return  true;

  }

  public static boolean validateEvent(String log) {
    //timestamp| {}

    String[] contents = log.split("\\|");

    String timestamp = contents[0];


    if(timestamp ==null || timestamp.trim().length() != 13 || !NumberUtils.isDigits(timestamp.trim())){
      return false;
    }

    String logEvent = contents[1];

    if(!logEvent.startsWith("{") || !logEvent.endsWith("}")){
      return false;
    }
    return  true;
  }
}
