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
