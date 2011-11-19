package com.sensei.search.query;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.sensei.search.query.filters.FilterConstructor;


public class UIDQueryConstructor extends QueryConstructor
{
  public static final String QUERY_TYPE = "ids";

  @Override
  public Query constructQuery(JSONObject jsonQuery) throws JSONException
  {
    FilterConstructor filterConstructor = FilterConstructor.getFilterConstructor(QUERY_TYPE);
    Filter filter = null;
    try
    {
      filter = filterConstructor.constructFilter(jsonQuery);
    }
    catch(Exception e)
    {
      throw new JSONException(e);
    }
    ConstantScoreQuery query = new ConstantScoreQuery(filter);
    query.setBoost((float)jsonQuery.optDouble("boost", 1.0));
    return query;
  }
}
