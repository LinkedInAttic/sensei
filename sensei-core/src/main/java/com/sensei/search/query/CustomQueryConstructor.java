package com.sensei.search.query;

import org.apache.lucene.search.Query;

import org.json.JSONException;
import org.json.JSONObject;

import com.sensei.plugin.SenseiPlugin;

public class CustomQueryConstructor extends QueryConstructor
{
  public static final String QUERY_TYPE = "custom";

  // "term" : {
  //   "color" : "red"
  // 
  //   // or "color" : {"term" : "red", "boost": 2.0}
  // },

  @Override
  protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException
  {
    try
    {
      String className = jsonQuery.getString("class");
      Class queryClass = Class.forName(className);

      Object q = queryClass.newInstance();
      ((SenseiPlugin)q).initialize(jsonQuery.optJSONObject("params"));

      ((Query)q).setBoost((float)jsonQuery.optDouble("boost", 1.0));
      return (Query)q;
    }
    catch(Throwable t)
    {
      throw new IllegalArgumentException(t.getMessage());
    }
  }
}
