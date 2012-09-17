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
package com.senseidb.search.query.filters;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Filter;
import org.json.JSONArray;
import org.json.JSONObject;

import com.browseengine.bobo.facets.filter.AndFilter;
import com.browseengine.bobo.facets.filter.NotFilter;
import com.browseengine.bobo.facets.filter.OrFilter;


public class BooleanFilterConstructor extends FilterConstructor
{
  public static final String FILTER_TYPE = "bool";

  // "bool" : {
  //   "must" : {
  //     "term" : { "tag" : "wow" }
  //   },
  //   "must_not" : {
  //     "range" : {
  //     "age" : { "from" : 10, "to" : 20 }
  //     }
  //   },
  //   "should" : [
  //      {
  //        "term" : { "tag" : "sometag" }
  //      },
  //      {
  //        "term" : { "tag" : "sometagtag" }
  //      }
  //   ]
  // },

  private QueryParser _qparser;

  public BooleanFilterConstructor(QueryParser qparser)
  {
    _qparser = qparser;
  }

  @Override
  protected Filter doConstructFilter(Object param) throws Exception
  {
    JSONObject json = (JSONObject)param;
    Object obj = json.opt(MUST_PARAM);
    List<Filter> andFilters = new ArrayList<Filter>();
    if (obj != null)
    {
      if (obj instanceof JSONArray)
      {
        for (int i=0; i<((JSONArray)obj).length(); ++i)
        {
          andFilters.add(FilterConstructor.constructFilter(((JSONArray)obj).getJSONObject(i),
                                                           _qparser));
        }
      }
      else if (obj instanceof JSONObject)
      {
        andFilters.add(FilterConstructor.constructFilter((JSONObject)obj, _qparser));
      }
    }
    obj = json.opt(MUST_NOT_PARAM);
    if (obj != null)
    {
      if (obj instanceof JSONArray)
      {
        for (int i=0; i<((JSONArray)obj).length(); ++i)
        {
          andFilters.add(
            new NotFilter(FilterConstructor.constructFilter(((JSONArray)obj).getJSONObject(i),
                                                            _qparser)));
        }
      }
      else if (obj instanceof JSONObject)
      {
        andFilters.add(new NotFilter(FilterConstructor.constructFilter((JSONObject)obj, _qparser)));
      }
    }
    JSONArray array = json.optJSONArray(SHOULD_PARAM);
    if (array != null)
    {
      List<Filter> orFilters = new ArrayList<Filter>(array.length());
      for (int i=0; i<array.length(); ++i)
      {
        orFilters.add(FilterConstructor.constructFilter(array.getJSONObject(i), _qparser));
      }
      if (orFilters.size() > 0)
        andFilters.add(new OrFilter(orFilters));
    }

    if (andFilters.size() > 0)
      return new AndFilter(andFilters);
    else
      return null;
  }
}
