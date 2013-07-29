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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.json.JSONObject;

import com.senseidb.search.query.QueryConstructor;

import java.io.IOException;

public class QueryFilterConstructor extends FilterConstructor{
  public static final String FILTER_TYPE = "query";

  private QueryParser _qparser;

  public QueryFilterConstructor(QueryParser qparser)
  {
    _qparser = qparser;
  }

	@Override
	protected SenseiFilter doConstructFilter(Object json) throws Exception {
		Query q = QueryConstructor.constructQuery((JSONObject)json, _qparser);

    if (q == null)
      return null;

    final QueryWrapperFilter queryWrapperFilter = new QueryWrapperFilter(q);
    return new SenseiFilter() {
      @Override
      public SenseiDocIdSet getSenseiDocIdSet(IndexReader reader) throws IOException {
        int cardinalityEstimate = reader.maxDoc() >> 1;
        return new SenseiDocIdSet(queryWrapperFilter.getDocIdSet(reader), cardinalityEstimate);
      }
    };
	}
	
}
