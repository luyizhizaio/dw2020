package com.kyrie.dw.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 日志类型过滤器：为了把不同类型的日志写到不同的channel中，在event的header 中增加一个标记
 */
public class LogTypeInterceptor implements Interceptor {
  @Override
  public void initialize() {

  }

  @Override
  public Event intercept(Event event) {

    Map<String, String> headers = event.getHeaders();

    byte[] body = event.getBody();

    String log = new String(body, Charset.forName("UTF-8"));

    //区分事件类型放入不同的标记
    if(log.contains("start")){
      headers.put("topic","topic_start");
    }else{
      headers.put("topic","topic_event");
    }
    return event;
  }

  @Override
  public List<Event> intercept(List<Event> list) {

    ArrayList<Event> newEvents = new ArrayList<>();
    //遍历调用直接返回
    for (Event event : list) {
      Event intercept1 = intercept(event);
      newEvents.add(intercept1);
    }
    return newEvents;
  }

  @Override
  public void close() {

  }

  public static class Builder implements  Interceptor.Builder{

    @Override
    public Interceptor build() {
      return  new LogTypeInterceptor();
    }

    @Override
    public void configure(Context context) {

    }
  }
}
