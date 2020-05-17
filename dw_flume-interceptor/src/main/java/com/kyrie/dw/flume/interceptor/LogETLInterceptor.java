package com.kyrie.dw.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * 自定义拦截器：对日志进行校验
 */
public class LogETLInterceptor implements Interceptor {

  @Override
  public void initialize() {

  }

  /**
   * 拦截事件
   * @param event
   * @return
   */
  @Override
  public Event intercept(Event event) {

    /**
     事件分为 header，body 从事件body取出内容
     */
    byte[] body = event.getBody();

    String log = new String(body, Charset.forName("UTF-8"));

    //区分事件校验
    if(log == null){
      return null;
    }

    if(log.contains("start")){
      if(LogUtils.validateStart(log)){
        return event;
      }
    }else{
      if(LogUtils.validateEvent(log)){
        return event;
      }
    }
    return null;
  }

  @Override
  public List<Event> intercept(List<Event> events) {
    //调用单个事件方法，处理多个事件

    ArrayList<Event> newEvents = new ArrayList<>();

    for (Event event : events) {
      Event intercept1 = intercept(event);

      if(intercept1 != null){
        newEvents.add(intercept1);
      }
    }

    return newEvents;
  }

  @Override
  public void close() {

  }

  //用于创建拦截器实例
  public static class Builder implements Interceptor.Builder{

    @Override
    public Interceptor build() {
      return new LogETLInterceptor();
    }

    @Override
    public void configure(Context context) {

    }
  }
}
