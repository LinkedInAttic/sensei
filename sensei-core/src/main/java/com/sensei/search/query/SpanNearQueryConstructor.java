package com.sensei.search.query;

import java.util.ArrayList;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class SpanNearQueryConstructor extends QueryConstructor {

	@Override
	public Query constructQuery(JSONObject jsonQuery) throws JSONException {

		
//	    "span_near" : {
//        "clauses" : [
//            { "span_term" : { "field" : "value1" } },
//            { "span_term" : { "field" : "value2" } },
//            { "span_term" : { "field" : "value3" } }
//        ],
//        "slop" : 12,
//        "in_order" : false,
//        "collect_payloads" : false
//    },
		
		JSONArray jsonArray = jsonQuery.getJSONArray("clauses");
		ArrayList<SpanTermQuery> clausesList = new ArrayList<SpanTermQuery>();
		for(int i = 0; i<jsonArray.length(); i++){
			JSONObject json = jsonArray.getJSONObject(i).getJSONObject("span_term");
			String field = (String)(json.keys().next());
			String value = (String)json.get(field);
			clausesList.add(new SpanTermQuery(new Term(field, value)));
		}
		
		SpanQuery[] clauses = clausesList.toArray(new SpanQuery[clausesList.size()]);
		
		int slop = jsonQuery.getInt("slop");
		boolean inOrder = jsonQuery.getBoolean("in_order");
		boolean collectPayloads = jsonQuery.getBoolean("collect_payloads");
		
		return new SpanNearQuery(clauses, slop, inOrder, collectPayloads);
	}

}
