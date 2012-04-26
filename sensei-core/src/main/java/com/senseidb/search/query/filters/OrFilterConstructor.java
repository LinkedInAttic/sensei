package com.senseidb.search.query.filters;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Filter;
import org.json.JSONArray;
import org.json.JSONObject;

import com.linkedin.bobo.facets.filter.OrFilter;


public class OrFilterConstructor extends FilterConstructor {
  public static final String FILTER_TYPE = "or";

  // "or" : [
  //   {
  //     "term" : { "color" : "red","_noOptimize" : false}
  //   },
  //   {
  //     "term" : { "category" : "van","_noOptimize" : false}
  //   }
  // ],

  private QueryParser _qparser;

  public OrFilterConstructor(QueryParser qparser)
  {
    _qparser = qparser;
  }

  @Override
  protected Filter doConstructFilter(Object obj) throws Exception {
    JSONArray filterArray = (JSONArray)obj;
    List<Filter> filters = new ArrayList<Filter>(filterArray.length());
    for (int i=0; i<filterArray.length(); ++i)
    {
      Filter filter = FilterConstructor.constructFilter(filterArray.getJSONObject(i), _qparser);
      if (filter != null)
        filters.add(filter);
    }
    return new OrFilter(filters);
  }
}
