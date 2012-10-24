package com.senseidb.search.query.filters;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.json.JSONObject;

import com.senseidb.search.query.QueryConstructor;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;

public class ConstExpFilterConstructor extends FilterConstructor
{
  public static final String FILTER_TYPE = "const_exp";

  @Override
  protected Filter doConstructFilter(Object json) throws Exception
  {
    Query q = QueryConstructor.constructQuery(new FastJSONObject().put(FILTER_TYPE, (JSONObject)json), null);
    if (q == null)
      return null;
    return new QueryWrapperFilter(q);
  } 

}
