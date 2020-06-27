package com.dw.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;

public class EventJsonUDTF extends GenericUDTF {

  //该方法中，我们将指定输出参数的名称和参数类型：
  @Override
  public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

    ArrayList<String> fieldNames = new ArrayList<>();

    ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();

    fieldNames.add("event_name");
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

    fieldNames.add("event_json");
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
  }

  //输入 1 条记录，输出若干条结果;
  @Override
  public void process(Object[] objects) throws HiveException {

    // 获取传入的 et
    String input = objects[0].toString();

    if(StringUtils.isBlank(input)){
      return;
    }else{
      try {
        JSONArray ja = new JSONArray(input);

        if (ja == null) {
          return;
        }
        for (int i = 0; i < ja.length(); i++) {
          String[] result = new String[2];

          try {
            // 取出每个的事件名称（ad/facoriters）
            result[0] = ja.getJSONObject(i).getString("en");
            // 取出每一个事件整体
            result[1] = ja.getString(i);
          } catch (JSONException e) {
            continue;
          }
          //返回结果:返回事件名称和事件json对象
          forward(result);
        }
      }catch (JSONException e){
        e.printStackTrace();
      }
    }
  }
  @Override
  public void close() throws HiveException {
  }
}
