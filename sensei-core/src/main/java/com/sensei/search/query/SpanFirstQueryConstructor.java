package com.sensei.search.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.json.JSONException;
import org.json.JSONObject;

public class SpanFirstQueryConstructor extends QueryConstructor {

	@Override
	public Query constructQuery(JSONObject jsonQuery) throws JSONException {

		
//	    "span_first" : {
//        "match" : {
//            "span_term" : { "color" : "red" }
//        },
//        "end" : 3
//    },
		

		JSONObject spanJson = jsonQuery.getJSONObject("match").getJSONObject("span_term");
		String field = (String) (spanJson.keys().next());
		String spanterm = (String) spanJson.getString(field);
		SpanQuery sq = new SpanTermQuery(new Term(field, spanterm));		
		
		int end = jsonQuery.optInt("end", 3);
		
		return new SpanFirstQuery(sq, end);
	}

}
