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
