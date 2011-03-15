package com.sensei.search.nodes.impl;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.json.JSONObject;

import com.sensei.search.nodes.SenseiQueryBuilder;

public class DefaultJsonQueryBuilderFactory extends
		AbstractJsonQueryBuilderFactory {

	private final QueryParser _qparser;
	public DefaultJsonQueryBuilderFactory(QueryParser qparser) {
		_qparser = qparser;
	}

	
	@Override
	protected SenseiQueryBuilder buildQuery(JSONObject jsonQuery) {
		final String queryString = jsonQuery == null ? null : jsonQuery.optString("query");
	
		return new SenseiQueryBuilder(){

			@Override
			public Filter buildFilter() throws ParseException {
				return null;
			}

			@Override
			public Query buildQuery() throws ParseException {
				if (queryString!=null && queryString.length()>0){
					return _qparser.parse(queryString);
				}
				else{
					return new MatchAllDocsQuery();
				}
			}
			
		};
	}
}
