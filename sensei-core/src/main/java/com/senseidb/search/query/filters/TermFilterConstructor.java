package com.senseidb.search.query.filters;

import java.util.Iterator;

import org.apache.lucene.search.Filter;
import org.json.JSONObject;


public class TermFilterConstructor extends FilterConstructor{
  public static final String FILTER_TYPE = "term";

  @Override
  protected Filter doConstructFilter(Object param) throws Exception {
    JSONObject json = (JSONObject)param;

    Iterator<String> iter = json.keys();
    if (!iter.hasNext())
      return null;

    String field = iter.next();
    String text;
    boolean noOptimize = false;

    if (NOOPTIMIZE_PARAM.equals(field))
    {
      noOptimize = json.optBoolean(NOOPTIMIZE_PARAM, false);
      field = iter.next();
    }

    Object obj = json.get(field);
    if (obj instanceof JSONObject)
    {
      text = ((JSONObject)obj).getString(VALUE_PARAM);
      noOptimize = json.optBoolean(NOOPTIMIZE_PARAM, false);
    }
    else
    {
      text = String.valueOf(obj);
    }

    return new SenseiTermFilter(field, new String[]{text}, null, false, noOptimize);
  }
  
}
