package com.sensei.search.query.filters;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.json.JSONObject;

import com.sensei.search.query.FilterConstructor;
import com.sensei.search.query.QueryConstructor;

public class QueryFilterConstructor extends FilterConstructor{

	@Override
	public Filter constructFilter(JSONObject json) throws Exception {
		JSONObject queryObj = json.getJSONObject("query");
		String type = (String)queryObj.keys().next();
		QueryConstructor qconstructor = QueryConstructor.getQueryConstructor(type);
		if (qconstructor == null){
			throw new IllegalArgumentException("unknow query type: "+type);
		}
		Query q = qconstructor.constructQuery(queryObj.optJSONObject(type));
		return new QueryWrapperFilter(q);
	}
	
}
