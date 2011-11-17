package com.sensei.search.query;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.json.JSONException;
import org.json.JSONObject;

public abstract class QueryConstructor {
	abstract public Query constructQuery(JSONObject params) throws JSONException;
	
	public static QueryConstructor getQueryConstructor(String type){
		return null;
	}
	
	public class TermQueryConstructor extends QueryConstructor{

		@Override
		public Query constructQuery(JSONObject params) {
			// TODO Auto-generated method stub
			return null;
		}
	}
	
	public static class MatchAllQueryConstructor extends QueryConstructor{

		@Override
		public Query constructQuery(JSONObject params) {
			double boost = params.optDouble("boost",1.0);
			
			MatchAllDocsQuery q = new MatchAllDocsQuery();
			q.setBoost((float)boost);
			
			return q;
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
