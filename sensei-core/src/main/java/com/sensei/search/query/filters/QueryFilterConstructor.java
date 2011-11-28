package com.sensei.search.query.filters;

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.json.JSONObject;

import com.sensei.search.query.QueryConstructor;

public class QueryFilterConstructor extends FilterConstructor{
  public static final String FILTER_TYPE = "query";

  private QueryParser _qparser;

  public QueryFilterConstructor(QueryParser qparser)
  {
    _qparser = qparser;
  }

	@Override
	protected Filter doConstructFilter(Object json) throws Exception {
		JSONObject queryObj = ((JSONObject)json).getJSONObject(QUERY_PARAM);
		Query q = QueryConstructor.constructQuery(queryObj, _qparser);
    if (q == null)
      return null;
		return new QueryWrapperFilter(q);
	}
	
}
