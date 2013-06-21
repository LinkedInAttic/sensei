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
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.json.JSONException;
import org.json.JSONObject;

public class SpanFirstQueryConstructor extends QueryConstructor {

  public static final String QUERY_TYPE = "span_first";
  
	@Override
  protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException
  {

		
//	    "span_first" : {
//        "match" : {
//            "span_term" : { "color" : "red" }
//        },
//        "end" : 3
//    },
		

		JSONObject spanJson = jsonQuery.getJSONObject(MATCH_PARAM).getJSONObject(SPAN_TERM_PARAM);
		String field = (String) (spanJson.keys().next());
		String spanterm = (String) spanJson.getString(field);
		SpanQuery sq = new SpanTermQuery(new Term(field, spanterm));		
		
		int end = jsonQuery.optInt(END_PARAM, 3);
		
		return new SpanFirstQuery(sq, end);
	}

}
