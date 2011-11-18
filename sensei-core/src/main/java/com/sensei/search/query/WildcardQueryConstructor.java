package com.sensei.search.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.json.JSONException;
import org.json.JSONObject;

public class WildcardQueryConstructor extends QueryConstructor {

  public static final String QUERY_TYPE = "wildcard";
	@Override
	public Query constructQuery(JSONObject jsonQuery) throws JSONException{

//		   "wildcard" : { 
//	       "user" : "ki*y" 
//
//	       // or 
//	       //"user" : { "value" : "ki*y", "boost" : 2.0 } 
//	   },
		
		
		String field = (String)(jsonQuery.keys().next());
		String wildcard = null;
		Object value = jsonQuery.get(field);
		
		if(value instanceof JSONObject){
			wildcard = ((JSONObject)value).optString("value", "");
			float boost = (float)((JSONObject)value).optDouble("boost", 2.0);
			Query query = new WildcardQuery(new Term(field, wildcard));
			query.setBoost(boost);
			return query;
		}else{
			wildcard = (String) value;
			return new WildcardQuery(new Term(field, wildcard));
		}
	}

}
