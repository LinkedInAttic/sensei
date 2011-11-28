package com.sensei.search.query;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.sensei.search.query.filters.FilterConstructor;


public class RangeQueryConstructor extends QueryConstructor
{
  public static final String QUERY_TYPE = "range";

  // "range" : {
  //     "age" : { 
  //         "from" : 10, 
  //         "to" : 20, 
  //         "boost" : 2.0,
  //         "_noOptimize" : false
  //     }
  // },

  @Override
  protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException
  {
    Filter filter = null;
    try
    {
      filter = FilterConstructor.constructFilter(jsonQuery, null/* QueryParser is not used by this filter */);
    }
    catch(Exception e)
    {
      throw new JSONException(e);
    }
    ConstantScoreQuery query = new ConstantScoreQuery(filter);
    query.setBoost((float)jsonQuery.getJSONObject((String)jsonQuery.keys().next()).optDouble("boost", 1.0));
    return query;
  }
}
