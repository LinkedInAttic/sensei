package com.sensei.search.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.json.JSONException;
import org.json.JSONObject;

public class SpanNotQueryConstructor extends QueryConstructor {

  public static final String QUERY_TYPE = "span_not";
	@Override
  protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException
  {

		
//	    "span_not" : {
//        "include" : {
//            "span_term" : { "field" : "value1" }
//        },
//        "exclude" : {
//            "span_term" : { "field" : "value2" }
//        }
//    },
		
		JSONObject jsonInclude = jsonQuery.getJSONObject(INCLUDE_PARAM);
		JSONObject jsonExclude = jsonQuery.getJSONObject(EXCLUDE_PARAM);
		
		JSONObject jsonInc = jsonInclude.getJSONObject(SPAN_TERM_PARAM);
		String fieldInc = (String)(jsonInc.keys().next());
		String valueInc = (String)jsonInc.get(fieldInc);
		SpanQuery sInc = new SpanTermQuery(new Term(fieldInc, valueInc));

		JSONObject jsonExc = jsonExclude.getJSONObject(SPAN_TERM_PARAM);
		String fieldExc = (String)(jsonExc.keys().next());
		String valueExc = (String)jsonExc.get(fieldExc);
		SpanQuery sExc = new SpanTermQuery(new Term(fieldExc, valueExc));
		
		if(!fieldInc.equals(fieldExc))
		  throw new IllegalArgumentException("Clauses must have same field: " + jsonQuery);
		
		SpanNotQuery snq = new SpanNotQuery(sInc, sExc);
		return snq;
	}

}
