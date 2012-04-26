package com.senseidb.search.client.req.relevance;

import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.client.json.JsonHandler;
import com.senseidb.search.client.json.JsonSerializer;

public class RelevanceValuesHandler implements JsonHandler<RelevanceValues>{

  @Override
  public JSONObject serialize(RelevanceValues bean) throws JSONException {
   if (bean == null) {
     return null;
   }
    return (JSONObject) JsonSerializer.serialize(bean.values);
  }

  @Override
  public RelevanceValues deserialize(JSONObject json) throws JSONException {
    throw new UnsupportedOperationException();
  }

}
