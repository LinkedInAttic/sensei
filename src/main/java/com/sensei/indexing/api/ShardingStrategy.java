package com.sensei.indexing.api;

import org.json.JSONObject;
import org.json.JSONException;

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
      return (int)(dataObj.getLong(_field) % maxShardId);
    }
  }
}
