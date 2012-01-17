package com.senseidb.search.query;

import java.util.ArrayList;

import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class DistMaxQueryConstructor extends QueryConstructor {
    private TermQueryConstructor termQueryConstructor = new TermQueryConstructor();
  public static final String QUERY_TYPE = "dis_max";
	@Override
  protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException
  {

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

	   JSONArray jsonArray = jsonQuery.getJSONArray(QUERIES_PARAM);
	   ArrayList<Query> ar = new ArrayList<Query>();

	    for(int i = 0; i<jsonArray.length(); i++){
	      JSONObject json = jsonArray.getJSONObject(i).getJSONObject(TERM_PARAM);
	      ar.add(termQueryConstructor.doConstructQuery(json));
	    }

	    float tieBreakerMultiplier = (float) jsonQuery.optDouble(TIE_BREAKER_PARAM, .0);
	    float boost = (float) jsonQuery.optDouble(BOOST_PARAM, 1.0);
	    Query dmq = new DisjunctionMaxQuery(ar, tieBreakerMultiplier);
	    dmq.setBoost(boost);

		return dmq;
	}

}
