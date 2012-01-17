package com.senseidb.search.query;

import java.util.Iterator;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.json.JSONException;
import org.json.JSONObject;

public class PrefixQueryConstructor extends QueryConstructor {

  public static final String QUERY_TYPE = "prefix";
  
	@Override
  protected Query doConstructQuery(JSONObject json) throws JSONException
  {

//		   "prefix" : { 
//	       "user" : "ki" 
//
//	       // or
//	       // "user" : {"value" : "ki", "boost" : 2.0 } 
//	   },

    Iterator<String> iter = json.keys();
    if (!iter.hasNext())
      throw new IllegalArgumentException("no term value specified: " + json);

    String field = iter.next();
		String prefix = null;
		Object obj = json.get(field);
		
		if(obj instanceof JSONObject){
			prefix = ((JSONObject)obj).optString(VALUE_PARAM, "");
			float boost = (float)((JSONObject)obj).optDouble(BOOST_PARAM, 1.0);
			Query query = new PrefixQuery(new Term(field, prefix));
			query.setBoost(boost);
			return query;
		}else{
			prefix = String.valueOf(obj);
			return new PrefixQuery(new Term(field, prefix));
		}
	}

}
