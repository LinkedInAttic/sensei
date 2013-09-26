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

import com.senseidb.search.query.MatchNoneDocsQuery;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.json.JSONObject;

import com.senseidb.search.query.QueryConstructor;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;

import java.io.IOException;

public class ConstExpFilterConstructor extends FilterConstructor
{
  public static final String FILTER_TYPE = "const_exp";

  @Override
  protected SenseiFilter doConstructFilter(Object json) throws Exception
  {
    Query q = QueryConstructor.constructQuery(new FastJSONObject().put(FILTER_TYPE, (JSONObject)json), null);
    if (q == null)
      return null;

    final QueryWrapperFilter filter = new QueryWrapperFilter(q);

    if(q instanceof MatchAllDocsQuery) {
      return new SenseiFilter() {
        @Override
        public SenseiDocIdSet getSenseiDocIdSet(IndexReader reader) throws IOException {
          return new SenseiDocIdSet(filter.getDocIdSet(reader), reader.maxDoc());
        }
      };
    } else if(q instanceof MatchNoneDocsQuery) {
      return new SenseiFilter() {
        @Override
        public SenseiDocIdSet getSenseiDocIdSet(IndexReader reader) throws IOException {
          return new SenseiDocIdSet(filter.getDocIdSet(reader), 0);
        }
      };
    } else {
      return SenseiFilter.buildDefault(filter);
    }
  }

}
