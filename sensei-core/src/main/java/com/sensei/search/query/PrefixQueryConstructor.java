package com.sensei.search.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.json.JSONException;
import org.json.JSONObject;

public class PrefixQueryConstructor extends QueryConstructor {

  public static final String QUERY_TYPE = "prefix";
  
	@Override
	public Query constructQuery(JSONObject jsonQuery) throws JSONException {

//		   "prefix" : { 
//	       "user" : "ki" 
//
//	       // or
//	       // "user" : {"value" : "ki", "boost" : 2.0 } 
//	   },
		
		
		String field = (String)(jsonQuery.keys().next());
		String prefix = null;
		Object value = jsonQuery.get(field);
		
		if(value instanceof JSONObject){
			prefix = ((JSONObject)value).optString("value", "");
			float boost = (float)((JSONObject)value).optDouble("boost", 2.0);
			Query query = new PrefixQuery(new Term(field, prefix));
			query.setBoost(boost);
			return query;
		}else{
			prefix = (String) value;
			return new PrefixQuery(new Term(field, prefix));
		}
	}

}
