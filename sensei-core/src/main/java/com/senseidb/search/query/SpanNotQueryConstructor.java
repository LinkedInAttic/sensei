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
