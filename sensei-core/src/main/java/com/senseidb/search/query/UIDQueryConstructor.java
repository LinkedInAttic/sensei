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

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.query.filters.FilterConstructor;


public class UIDQueryConstructor extends QueryConstructor
{
  public static final String QUERY_TYPE = "ids";

  @Override
  protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException
  {
    JSONObject filterJson = new JSONObject();
    filterJson.put(QUERY_TYPE, jsonQuery);

    Filter filter = null;
    try
    {
      filter = FilterConstructor.constructFilter(filterJson, null/* QueryParser is not used by this filter */);
    }
    catch(Exception e)
    {
      throw new JSONException(e);
    }
    ConstantScoreQuery query = new ConstantScoreQuery(filter);
    query.setBoost((float)jsonQuery.optDouble(BOOST_PARAM, 1.0));
    return query;
  }
}
