/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
package com.senseidb.search.query;

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

  public static final String QUERY_TYPE = "span_near";
	@Override
  public Query doConstructQuery(JSONObject jsonQuery) throws JSONException
  {

		
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
		
	    String fieldCheck = null;
		JSONArray jsonArray = jsonQuery.getJSONArray(CLAUSES_PARAM);
		ArrayList<SpanTermQuery> clausesList = new ArrayList<SpanTermQuery>();
		for(int i = 0; i<jsonArray.length(); i++){
			JSONObject json = jsonArray.getJSONObject(i).getJSONObject(SPAN_TERM_PARAM);
			String field = (String)(json.keys().next());
			
            if(fieldCheck == null)
              fieldCheck = field;
            else if( !fieldCheck.equals(field))
              throw new IllegalArgumentException("Clauses must have same field: " + jsonQuery);

			String value = (String)json.get(field);
			clausesList.add(new SpanTermQuery(new Term(field, value)));
		}
		
		SpanQuery[] clauses = clausesList.toArray(new SpanQuery[clausesList.size()]);
		
		int slop = jsonQuery.optInt(SLOP_PARAM, 12);
		boolean inOrder = jsonQuery.optBoolean(IN_ORDER_PARAM, false);
		boolean collectPayloads = jsonQuery.optBoolean(COLLECT_PAYLOADS_PARAM, false);
		
		return new SpanNearQuery(clauses, slop, inOrder, collectPayloads);
	}

}
