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

import com.browseengine.bobo.facets.filter.AndFilter;


public class AndFilterConstructor extends FilterConstructor {
  public static final String FILTER_TYPE = "and";

  // "and" : [
  //   {
  //     "term" : { "color" : "red","_noOptimize" : false},
  //   },
  //   {
  //     "query" : {
  //        "query_string" : { 
  //             "query" : "this AND that OR thus"
  //        }
  //     }
  //   }
  // ],

  private QueryParser _qparser;

  public AndFilterConstructor(QueryParser qparser)
  {
    _qparser = qparser;
  }

  @Override
  protected Filter doConstructFilter(Object obj) throws Exception
  {
    JSONArray filterArray = (JSONArray)obj;
    List<Filter> filters = new ArrayList<Filter>(filterArray.length());
    for (int i=0; i<filterArray.length(); ++i)
    {
      Filter filter = FilterConstructor.constructFilter(filterArray.getJSONObject(i), _qparser);
      if (filter != null)
        filters.add(filter);
    }
    return new AndFilter(filters);
  }
}
