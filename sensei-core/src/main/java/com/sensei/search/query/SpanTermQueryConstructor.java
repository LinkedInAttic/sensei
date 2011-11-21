package com.sensei.search.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.json.JSONException;
import org.json.JSONObject;

public class SpanTermQueryConstructor extends QueryConstructor {

  public static final String QUERY_TYPE = "span_term";
	@Override
  protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException
  {
		
		
//	    "span_term" : { 
//        "color" : "red" 
//
//     //or
//     // "user" : { "value" : "kimchy", "boost" : 2.0 } 
//    },
		
		String field = (String)(jsonQuery.keys().next());
		String spanterm = null;
		Object value = jsonQuery.get(field);
		
		if(value instanceof JSONObject){
			spanterm = ((JSONObject)value).optString("value", "");
			float boost = (float)((JSONObject)value).optDouble("boost", 2.0);
			Query query = new SpanTermQuery(new Term(field, spanterm));
			query.setBoost(boost);
			return query;
		}else{
			spanterm = (String) value;
			return new SpanTermQuery(new Term(field, spanterm));
		}
	}

}
