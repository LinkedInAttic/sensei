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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class BooleanQueryConstructor extends QueryConstructor
{
  public static final String QUERY_TYPE = "bool";

  // "bool" : {
  //     "must" : {
  //         "term" : { "color" : "red" }
  //     },
  //     "must_not" : {
  //         "range" : {
  //             "age" : { "from" : 10, "to" : 20 }
  //         }
  //     },
  //     "should" : [
  //         {
  //             "term" : { "tag" : "wow", "noOptimize" : false}
  //         },
  //         {
  //             "term" : { "tag" : "search"}
  //         }
  //     ],
  //     "minimum_number_should_match" : 1,
  //     "boost" : 1.0,
  //     "disable_coord" : false                      // optional: default = false
  // },

  private QueryParser _qparser;

  public BooleanQueryConstructor(QueryParser qparser)
  {
    _qparser = qparser;
  }

  @Override
  protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException
  {
    BooleanQuery query = new BooleanQuery(jsonQuery.optBoolean(DISABLE_COORD_PARAM, false));
    Object obj = jsonQuery.opt(MUST_PARAM);
    if (obj != null)
    {
      if (obj instanceof JSONArray)
      {
        for (int i=0; i<((JSONArray)obj).length(); ++i)
        {
          query.add(QueryConstructor.constructQuery(((JSONArray)obj).getJSONObject(i), _qparser),
                    BooleanClause.Occur.MUST);
        }
      }
      else if (obj instanceof JSONObject)
      {
        query.add(QueryConstructor.constructQuery((JSONObject)obj, _qparser), BooleanClause.Occur.MUST);
      }
    }
    obj = jsonQuery.opt(MUST_NOT_PARAM);
    if (obj != null)
    {
      if (obj instanceof JSONArray)
      {
        for (int i=0; i<((JSONArray)obj).length(); ++i)
        {
          query.add(QueryConstructor.constructQuery(((JSONArray)obj).getJSONObject(i), _qparser),
                    BooleanClause.Occur.MUST_NOT);
        }
      }
      else if (obj instanceof JSONObject)
      {
        query.add(QueryConstructor.constructQuery((JSONObject)obj, _qparser),
                  BooleanClause.Occur.MUST_NOT);
      }
    }

    JSONArray array = jsonQuery.optJSONArray(SHOULD_PARAM);
    if (array != null && array.length() > 0)
    {
      for (int i=0; i<array.length(); ++i)
      {
        query.add(QueryConstructor.constructQuery(array.getJSONObject(i), _qparser),
                  BooleanClause.Occur.SHOULD);
      }
      query.setMinimumNumberShouldMatch(jsonQuery.optInt(MINIMUM_NUMBER_SHOULD_MATCH_PARAM, 0));
    }

    query.setBoost((float)jsonQuery.optDouble(BOOST_PARAM, 1.0));

    return query;
  }
}
