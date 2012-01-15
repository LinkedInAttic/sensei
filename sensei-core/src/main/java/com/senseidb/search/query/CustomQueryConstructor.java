package com.senseidb.search.query;

import org.apache.lucene.search.Query;
import org.json.JSONException;
import org.json.JSONObject;

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
      String className = jsonQuery.getString(CLASS_PARAM);
      Class queryClass = Class.forName(className);

      Object q = queryClass.newInstance();
      //TODO add initialization
      //((SenseiPlugin)q).initialize(jsonQuery.optJSONObject(PARAMS_PARAM));

      ((Query)q).setBoost((float)jsonQuery.optDouble(BOOST_PARAM, 1.0));
      return (Query)q;
    }
    catch(Throwable t)
    {
      throw new IllegalArgumentException(t.getMessage());
    }
  }
}
