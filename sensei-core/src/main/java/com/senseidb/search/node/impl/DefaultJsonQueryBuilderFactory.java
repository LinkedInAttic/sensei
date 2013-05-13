package com.senseidb.search.node.impl;

import org.apache.log4j.Logger;
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
import com.senseidb.util.JSONUtil.FastJSONArray;
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
          return FilterConstructor.constructFilter(filter, _qparser);
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
