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

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.query.filters.FilterConstructor;


public class FilteredQueryConstructor extends QueryConstructor
{
  public static final String QUERY_TYPE = "filtered";

  // "filtered" : {
  //         // any query object
  //     "query" : {
  //         "term" : { "tag" : "wow" }
  //     },
  //        // any filter defined in the filters.json
  //     "filter" : {
  //         "range" : {
  //             "age" : { "from" : 10, "to" : 20 }
  //         }
  //     }
  // },

  private QueryParser _qparser;

  public FilteredQueryConstructor(QueryParser qparser)
  {
    _qparser = qparser;
  }

  @Override
  protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException
  {
    JSONObject queryJson  = jsonQuery.getJSONObject(QUERY_PARAM);
    JSONObject filterJson = jsonQuery.getJSONObject(FILTER_PARAM);

    if (queryJson == null || filterJson == null)
      throw new IllegalArgumentException("query and filter are both required: " + jsonQuery);

    Query query = null;
    try
    {
      query = QueryConstructor.constructQuery(queryJson, _qparser);
    }
    catch(Exception e)
    {
      throw new JSONException(e);
    }
    Filter filter = null;
    try
    {
      filter = FilterConstructor.constructFilter(filterJson, _qparser);
    }
    catch(Exception e)
    {
      throw new JSONException(e);
    }

    return new FilteredQuery(query, filter);
  }
}
