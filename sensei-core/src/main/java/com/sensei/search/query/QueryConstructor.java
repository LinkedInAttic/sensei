package com.sensei.search.query;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.Query;
import org.json.JSONException;
import org.json.JSONObject;

public abstract class QueryConstructor {
	abstract public Query constructQuery(JSONObject params) throws JSONException;
	
	private static final Map<String,QueryConstructor> QUERY_CONSTRUCTOR_MAP = new HashMap<String,QueryConstructor>();
	
	static{
	  QUERY_CONSTRUCTOR_MAP.put(DistMaxQueryConstructor.QUERY_TYPE, new DistMaxQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(PrefixQueryConstructor.QUERY_TYPE, new PrefixQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(WildcardQueryConstructor.QUERY_TYPE, new WildcardQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanFirstQueryConstructor.QUERY_TYPE, new SpanFirstQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanNearQueryConstructor.QUERY_TYPE, new SpanNearQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanNotQueryConstructor.QUERY_TYPE, new SpanNotQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanOrQueryConstructor.QUERY_TYPE, new SpanOrQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanTermQueryConstructor.QUERY_TYPE, new SpanTermQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(MatchAllQueryConstructor.QUERY_TYPE, new MatchAllQueryConstructor());
	}
	
	public static QueryConstructor getQueryConstructor(String type){
		return QUERY_CONSTRUCTOR_MAP.get(type);
	}
	
	public class TermQueryConstructor extends QueryConstructor{

		@Override
		public Query constructQuery(JSONObject params) {
			// TODO Auto-generated method stub
			return null;
		}
	}
	
	public static class TextQueryConstructor extends QueryConstructor{

		@Override
		public Query constructQuery(JSONObject params) {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
}
