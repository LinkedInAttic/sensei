package com.sensei.search.query;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.json.JSONException;
import org.json.JSONObject;

public abstract class QueryConstructor
{
  abstract public Query constructQuery(JSONObject params) throws JSONException;

  private static final Map<String,QueryConstructor> QUERY_CONSTRUCTOR_MAP = new HashMap<String,QueryConstructor>();

  static
  {
    QUERY_CONSTRUCTOR_MAP.put(DistMaxQueryConstructor.QUERY_TYPE, new DistMaxQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(PrefixQueryConstructor.QUERY_TYPE, new PrefixQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(WildcardQueryConstructor.QUERY_TYPE, new WildcardQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanFirstQueryConstructor.QUERY_TYPE, new SpanFirstQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanNearQueryConstructor.QUERY_TYPE, new SpanNearQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanNotQueryConstructor.QUERY_TYPE, new SpanNotQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanOrQueryConstructor.QUERY_TYPE, new SpanOrQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanTermQueryConstructor.QUERY_TYPE, new SpanTermQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(MatchAllQueryConstructor.QUERY_TYPE, new MatchAllQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(TermQueryConstructor.QUERY_TYPE, new TermQueryConstructor());
  }
  
  public static QueryConstructor getQueryConstructor(String type, Analyzer analyzer)
  {
    QueryConstructor queryConstructor = QUERY_CONSTRUCTOR_MAP.get(type);
    if (queryConstructor == null)
    {
      if (TextQueryConstructor.QUERY_TYPE.equals(type))
      {
        queryConstructor = new TextQueryConstructor(analyzer);
      }
      else if (BooleanQueryConstructor.QUERY_TYPE.equals(type))
      {
        queryConstructor = new BooleanQueryConstructor(analyzer);
      }
    }
    return queryConstructor;
  }
}
