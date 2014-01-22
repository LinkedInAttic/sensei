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
package com.senseidb.search.node.impl;

import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.json.JSONException;
import org.json.JSONObject;
import com.senseidb.search.node.SenseiQueryBuilder;
import com.senseidb.search.query.QueryConstructor;
import com.senseidb.search.query.filters.FilterConstructor;
import com.senseidb.search.query.filters.SenseiDocIdSet;
import com.senseidb.search.query.filters.SenseiFilter;
import com.senseidb.util.JSONUtil.FastJSONObject;

public class DefaultJsonQueryBuilderFactory extends
    AbstractJsonQueryBuilderFactory {
  private static Logger logger = Logger.getLogger(DefaultJsonQueryBuilderFactory.class);

  private final QueryParser _qparser;
  public DefaultJsonQueryBuilderFactory(QueryParser qparser) {
    _qparser = qparser;
  }

  @Override
  public SenseiQueryBuilder buildQueryBuilder(JSONObject jsonQuery) {
    final JSONObject query;
    final JSONObject filter;

    if (jsonQuery != null)
    {
      Object obj = jsonQuery.opt("query");
      if (obj == null)
        query = null;
      else if (obj instanceof JSONObject)
        query = (JSONObject)obj;
      else if (obj instanceof String)
      {
        query = new FastJSONObject();
        JSONObject tmp = new FastJSONObject();
        try
        {
          tmp.put("query", (String)obj);
          query.put("query_string", tmp);
        }
        catch (JSONException jse)
        {
          // Should never happen.
        }
      }
      else
      {
        throw new IllegalArgumentException("Query is not supported: " + jsonQuery);
      }

      filter = jsonQuery.optJSONObject("filter");
    }
    else
    {
      query = null;
      filter = null;
    }

    return new SenseiQueryBuilder(){

      @Override
      public Filter buildFilter() throws ParseException {
        try
        {
          final SenseiFilter senseiFilter = FilterConstructor.constructFilter(filter, _qparser);
          if (logger.isTraceEnabled() && senseiFilter != null) {
            return new SenseiFilter() {

              volatile boolean called = false;
              @Override
              public SenseiDocIdSet getSenseiDocIdSet(IndexReader reader) throws IOException {
                SenseiDocIdSet docIdSet = senseiFilter.getSenseiDocIdSet(reader);
                if(!called) {
                  logger.info("Running the query: " + (query == null ? "NULL" : query.toString()));
                  logger.info("Plan(" + (reader.maxDoc() + 1) + "): " + docIdSet.getQueryPlan());
                }
                return docIdSet;
              }
            };
          } else {
            return senseiFilter;
          }
        }
        catch (Exception e)
        {
          logger.error(e.getMessage(), e);
          throw new ParseException(e.getMessage());
        }
      }

      @Override
      public Query buildQuery() throws ParseException {
        try
        {
          Query q = QueryConstructor.constructQuery(query, _qparser);
          if (q == null)
          {
            q = new MatchAllDocsQuery();
          }
          return q;
        }
        catch (Exception e)
        {
          logger.error(e.getMessage(), e);
          throw new ParseException(e.getMessage());
        }
      }

    };
  }
}
