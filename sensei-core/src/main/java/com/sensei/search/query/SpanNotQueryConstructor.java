package com.sensei.search.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.json.JSONException;
import org.json.JSONObject;

public class SpanNotQueryConstructor extends QueryConstructor {

	@Override
	public Query constructQuery(JSONObject jsonQuery) throws JSONException {

		
//	    "span_not" : {
//        "include" : {
//            "span_term" : { "field1" : "value1" }
//        },
//        "exclude" : {
//            "span_term" : { "field2" : "value2" }
//        }
//    },
		
		JSONObject jsonInclude = jsonQuery.getJSONObject("include");
		JSONObject jsonExclude = jsonQuery.getJSONObject("exclude");
		
		JSONObject jsonInc = jsonInclude.getJSONObject("span_term");
		String fieldInc = (String)(jsonInc.keys().next());
		String valueInc = (String)jsonInc.get(fieldInc);
		SpanQuery sInc = new SpanTermQuery(new Term(fieldInc, valueInc));

		JSONObject jsonExc = jsonExclude.getJSONObject("span_term");
		String fieldExc = (String)(jsonExc.keys().next());
		String valueExc = (String)jsonExc.get(fieldExc);
		SpanQuery sExc = new SpanTermQuery(new Term(fieldExc, valueExc));		
		
		SpanNotQuery snq = new SpanNotQuery(sInc, sExc);
		return snq;
	}

}
