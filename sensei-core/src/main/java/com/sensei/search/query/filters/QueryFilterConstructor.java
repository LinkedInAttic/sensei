package com.sensei.search.query.filters;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.json.JSONObject;

import com.sensei.search.query.QueryConstructor;

public class QueryFilterConstructor extends FilterConstructor{
  private Analyzer _analyzer;

  public QueryFilterConstructor(Analyzer analyzer)
  {
    _analyzer = analyzer;
  }

	@Override
	public Filter constructFilter(JSONObject json) throws Exception {
		JSONObject queryObj = json.getJSONObject("query");
		String type = (String)queryObj.keys().next();
		QueryConstructor qconstructor = QueryConstructor.getQueryConstructor(type, _analyzer);
		if (qconstructor == null){
			throw new IllegalArgumentException("unknow query type: "+type);
		}
		Query q = qconstructor.constructQuery(queryObj.getJSONObject(type));
		return new QueryWrapperFilter(q);
	}
	
}
