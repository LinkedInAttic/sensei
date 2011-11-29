package com.sensei.search.query;

import java.util.Iterator;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.json.JSONException;
import org.json.JSONObject;

public class SpanTermQueryConstructor extends QueryConstructor {

  public static final String QUERY_TYPE = "span_term";
	@Override
  protected Query doConstructQuery(JSONObject json) throws JSONException
  {
		
		
//	    "span_term" : { 
//        "color" : "red" 
//
//     //or
//     // "user" : { "value" : "kimchy", "boost" : 2.0 } 
//    },

    Iterator<String> iter = json.keys();
    if (!iter.hasNext())
      throw new IllegalArgumentException("no term value specified: " + json);

		String field = iter.next();
		String spanterm = null;
		Object obj = json.get(field);
		
		if(obj instanceof JSONObject){
			spanterm = ((JSONObject)obj).optString(VALUE_PARAM, "");
			float boost = (float)((JSONObject)obj).optDouble(BOOST_PARAM, 2.0);
			Query query = new SpanTermQuery(new Term(field, spanterm));
			query.setBoost(boost);
			return query;
		}else{
			spanterm = String.valueOf(obj);
			return new SpanTermQuery(new Term(field, spanterm));
		}
	}

}
