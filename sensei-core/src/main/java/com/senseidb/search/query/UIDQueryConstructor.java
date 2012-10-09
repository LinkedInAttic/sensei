package com.senseidb.search.query;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.query.filters.FilterConstructor;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;


public class UIDQueryConstructor extends QueryConstructor
{
  public static final String QUERY_TYPE = "ids";

  @Override
  protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException
  {
    JSONObject filterJson = new FastJSONObject();
    filterJson.put(QUERY_TYPE, jsonQuery);

    Filter filter = null;
    try
    {
      filter = FilterConstructor.constructFilter(filterJson, null/* QueryParser is not used by this filter */);
    }
    catch(Exception e)
    {
      throw new JSONException(e);
    }
    ConstantScoreQuery query = new ConstantScoreQuery(filter);
    query.setBoost((float)jsonQuery.optDouble(BOOST_PARAM, 1.0));
    return query;
  }
}
