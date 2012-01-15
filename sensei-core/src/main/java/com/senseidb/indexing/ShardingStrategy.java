package com.senseidb.indexing;

import org.json.JSONObject;
import org.json.JSONException;

import com.senseidb.conf.SenseiSchema;

public interface ShardingStrategy {
  int caculateShard(int maxShardId,JSONObject dataObj) throws JSONException;

  public static class FieldModShardingStrategy implements ShardingStrategy
  {
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
        uid = dataObj.getLong(_field);
      else
        uid = event.getLong(_field);
      return (int)(uid % maxShardId);
    }
  }
}
