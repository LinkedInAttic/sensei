package com.sensei.search.query.filters;

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
  protected Filter doConstructFilter(Object obj) throws Exception
  {
    JSONObject json = (JSONObject)obj;
    JSONObject jsonObj = json.optJSONObject(MUST_PARAM);
    List<Filter> andFilters = new ArrayList<Filter>();
    if (jsonObj != null)
    {
      andFilters.add(FilterConstructor.constructFilter(jsonObj, _qparser));
    }
    jsonObj = json.optJSONObject(MUST_NOT_PARAM);
    if (jsonObj != null)
    {
      andFilters.add(new NotFilter(FilterConstructor.constructFilter(jsonObj, _qparser)));
    }
    JSONArray array = json.optJSONArray(SHOULD_PARAM);
    if (array != null)
    {
      List<Filter> orFilters = new ArrayList<Filter>(array.length());
      for (int i=0; i<array.length(); ++i)
      {
        jsonObj = array.getJSONObject(i);
        orFilters.add(FilterConstructor.constructFilter(jsonObj, _qparser));
      }
      andFilters.add(new OrFilter(orFilters));
    }

    return new AndFilter(andFilters);
  }
}
