package com.senseidb.search.query;

import java.util.Iterator;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.json.JSONException;
import org.json.JSONObject;

public class WildcardQueryConstructor extends QueryConstructor {

  public static final String QUERY_TYPE = "wildcard";
	@Override
  protected Query doConstructQuery(JSONObject json) throws JSONException
  {

//		   "wildcard" : { 
//	       "user" : "ki*y" 
//
//	       // or 
//	       //"user" : { "value" : "ki*y", "boost" : 2.0 } 
//	   },

    Iterator<String> iter = json.keys();
    if (!iter.hasNext())
      throw new IllegalArgumentException("no term value specified: " + json);

		String field = iter.next();
		String wildcard = null;
		Object obj = json.get(field);
		
		if(obj instanceof JSONObject){
			wildcard = ((JSONObject)obj).optString(VALUE_PARAM, "");
			float boost = (float)((JSONObject)obj).optDouble(BOOST_PARAM, 2.0);
			Query query = new WildcardQuery(new Term(field, wildcard));
			query.setBoost(boost);
			return query;
		}else{
			wildcard = String.valueOf(obj);
			return new WildcardQuery(new Term(field, wildcard));
		}
	}

}
