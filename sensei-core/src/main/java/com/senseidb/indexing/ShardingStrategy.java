package com.senseidb.indexing;

import java.util.Map;

import org.json.JSONObject;
import org.json.JSONException;

import com.senseidb.conf.SenseiSchema;

import com.senseidb.plugin.SenseiPluginFactory;
import com.senseidb.plugin.SenseiPluginRegistry;

public interface ShardingStrategy {
  int caculateShard(int maxShardId,JSONObject dataObj) throws JSONException;

  public static class FieldModShardingStrategy implements ShardingStrategy
  {
    public static class Factory implements SenseiPluginFactory<FieldModShardingStrategy>
    {
      @Override
      public FieldModShardingStrategy getBean(Map<String, String> initProperties,
                                              String fullPrefix,
                                              SenseiPluginRegistry pluginRegistry)
      {
        return new FieldModShardingStrategy(initProperties.get("field"));
      }
    }

    protected String _field;

    public FieldModShardingStrategy(String field)
    {
      _field = field;
    }

    @Override
    public int caculateShard(int maxShardId,JSONObject dataObj) throws JSONException
    {
      JSONObject event = dataObj.optJSONObject(SenseiSchema.EVENT_FIELD);
      long uid;
      if (event == null)
        uid = Long.parseLong(dataObj.getString(_field));
      else
        uid = Long.parseLong(event.getString(_field));
      return (int)(uid % maxShardId);
    }
  }
}
