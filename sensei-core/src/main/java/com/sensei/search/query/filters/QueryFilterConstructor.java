package com.sensei.search.query.filters;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.json.JSONObject;

import com.sensei.search.query.QueryConstructor;

public class QueryFilterConstructor extends FilterConstructor{
  public static final String FILTER_TYPE = "query";

  private Analyzer _analyzer;

  public QueryFilterConstructor(Analyzer analyzer)
  {
    _analyzer = analyzer;
  }

	@Override
	protected Filter doConstructFilter(JSONObject json) throws Exception {
		JSONObject queryObj = json.getJSONObject("query");
		Query q = QueryConstructor.constructQuery(queryObj, _analyzer);
		return new QueryWrapperFilter(q);
	}
	
}
