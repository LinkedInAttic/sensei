package com.sensei.search.query.filters;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.json.JSONObject;


public class AndFilterConstructor extends FilterConstructor {
  public static final String FILTER_TYPE = "and";

  @Override
  protected Filter doConstructFilter(final JSONObject json) throws Exception {
     return null;
  }

}
