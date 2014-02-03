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

import java.util.Iterator;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.json.JSONException;
import org.json.JSONObject;

public class PrefixQueryConstructor extends QueryConstructor {

  public static final String QUERY_TYPE = "prefix";
  
	@Override
  public Query doConstructQuery(JSONObject json) throws JSONException
  {

//		   "prefix" : { 
//	       "user" : "ki" 
//
//	       // or
//	       // "user" : {"value" : "ki", "boost" : 2.0 } 
//	   },

    Iterator<String> iter = json.keys();
    if (!iter.hasNext())
      throw new IllegalArgumentException("no term value specified: " + json);

    String field = iter.next();
		String prefix = null;
		Object obj = json.get(field);
		
		if(obj instanceof JSONObject){
			prefix = ((JSONObject)obj).optString(VALUE_PARAM, "");
			float boost = (float)((JSONObject)obj).optDouble(BOOST_PARAM, 1.0);
			Query query = new PrefixQuery(new Term(field, prefix));
			query.setBoost(boost);
			return query;
		}else{
			prefix = String.valueOf(obj);
			return new PrefixQuery(new Term(field, prefix));
		}
	}

}
