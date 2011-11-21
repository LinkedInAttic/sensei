package com.sensei.search.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.sensei.search.query.filters.FilterConstructor;


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

  private Analyzer _analyzer;

  public FilteredQueryConstructor(Analyzer analyzer)
  {
    _analyzer = analyzer;
  }

  @Override
  protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException
  {
    JSONObject queryJson  = jsonQuery.getJSONObject("query");
    JSONObject filterJson = jsonQuery.getJSONObject("filter");

    if (queryJson == null || filterJson == null)
      throw new IllegalArgumentException("query and filter are both required: " + jsonQuery);

    Query query = null;
    try
    {
      query = QueryConstructor.constructQuery(queryJson, _analyzer);
    }
    catch(Exception e)
    {
      throw new JSONException(e);
    }
    Filter filter = null;
    try
    {
      filter = FilterConstructor.constructFilter(filterJson, _analyzer);
    }
    catch(Exception e)
    {
      throw new JSONException(e);
    }

    return new FilteredQuery(query, filter);
  }
}
