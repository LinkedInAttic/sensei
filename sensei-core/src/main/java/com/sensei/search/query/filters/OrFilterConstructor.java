package com.sensei.search.query.filters;

import org.apache.lucene.search.Filter;
import org.json.JSONObject;


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

  @Override
  protected Filter doConstructFilter(JSONObject json) throws Exception {
    return null;
  }
}
