package com.senseidb.gateway.kafka;

import java.nio.charset.Charset;

import org.json.JSONObject;

import com.senseidb.indexing.DataSourceFilter;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;

public class DefaultJsonDataSourceFilter extends DataSourceFilter<DataPacket> {
  public final static Charset UTF8 = Charset.forName("UTF-8");
  
  @Override
  protected JSONObject doFilter(DataPacket packet) throws Exception {
    String jsonString = new String(packet.data,packet.offset,packet.size,UTF8);
    return new FastJSONObject(jsonString);
  }
}
