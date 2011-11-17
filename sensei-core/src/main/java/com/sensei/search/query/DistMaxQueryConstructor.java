package com.sensei.search.query;

import org.apache.lucene.search.Query;
import org.json.JSONException;
import org.json.JSONObject;

public class DistMaxQueryConstructor extends QueryConstructor {

	@Override
	public Query constructQuery(JSONObject params) throws JSONException {

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
		
		return null;
	}

}
