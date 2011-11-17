package com.sensei.search.query;

import java.util.ArrayList;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class DistMaxQueryConstructor extends QueryConstructor {

  public static final String QUERY_TYPE = "dis_max";
	@Override
	public Query constructQuery(JSONObject jsonQuery) throws JSONException {

//	    "dis_max" : {
//        "tie_breaker" : 0.7,
//        "boost" : 1.2,
//        "queries" : [
//            {
//                "term" : { "age" : 34 }
//            },
//            {
//                "term" : { "age" : 35 }
//            }
//        ]
//    },
		
	   JSONArray jsonArray = jsonQuery.getJSONArray("queries");
	   ArrayList<Query> ar = new ArrayList<Query>();
	   
	    for(int i = 0; i<jsonArray.length(); i++){
	      JSONObject json = jsonArray.getJSONObject(i).getJSONObject("term");
	      String field = (String)(json.keys().next());
	      String value = (String)json.get(field);
	      ar.add(new TermQuery(new Term(field, value)));
	    }
	    
	    float tieBreakerMultiplier = (float) jsonQuery.optDouble("tie_breaker", 0.7);
	    float boost = (float) jsonQuery.optDouble("boost", 1.2);
	    Query dmq = new DisjunctionMaxQuery(ar, tieBreakerMultiplier);
	    dmq.setBoost(boost);
	    
		return dmq;
	}

}
