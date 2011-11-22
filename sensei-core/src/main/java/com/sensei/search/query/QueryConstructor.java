package com.sensei.search.query;

import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.json.JSONException;
import org.json.JSONObject;

public abstract class QueryConstructor
{
  private static final Map<String,QueryConstructor> QUERY_CONSTRUCTOR_MAP = new HashMap<String,QueryConstructor>();

  static
  {
    QUERY_CONSTRUCTOR_MAP.put(DistMaxQueryConstructor.QUERY_TYPE, new DistMaxQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(PrefixQueryConstructor.QUERY_TYPE, new PrefixQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(WildcardQueryConstructor.QUERY_TYPE, new WildcardQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(RangeQueryConstructor.QUERY_TYPE, new RangeQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanFirstQueryConstructor.QUERY_TYPE, new SpanFirstQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanNearQueryConstructor.QUERY_TYPE, new SpanNearQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanNotQueryConstructor.QUERY_TYPE, new SpanNotQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanOrQueryConstructor.QUERY_TYPE, new SpanOrQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(SpanTermQueryConstructor.QUERY_TYPE, new SpanTermQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(MatchAllQueryConstructor.QUERY_TYPE, new MatchAllQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(TermQueryConstructor.QUERY_TYPE, new TermQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(TermsQueryConstructor.QUERY_TYPE, new TermsQueryConstructor());
    QUERY_CONSTRUCTOR_MAP.put(UIDQueryConstructor.QUERY_TYPE, new UIDQueryConstructor());
  }
  
  public static QueryConstructor getQueryConstructor(String type, QueryParser qparser)
  {
    QueryConstructor queryConstructor = QUERY_CONSTRUCTOR_MAP.get(type);
    if (queryConstructor == null)
    {
      if (QueryStringQueryConstructor.QUERY_TYPE.equals(type))
        queryConstructor = new QueryStringQueryConstructor(qparser);
      else if (TextQueryConstructor.QUERY_TYPE.equals(type))
        queryConstructor = new TextQueryConstructor(qparser);
      else if (BooleanQueryConstructor.QUERY_TYPE.equals(type))
        queryConstructor = new BooleanQueryConstructor(qparser);
      else if (FilteredQueryConstructor.QUERY_TYPE.equals(type))
        queryConstructor = new FilteredQueryConstructor(qparser);
    }
    return queryConstructor;
  }

  public static Query constructQuery(JSONObject jsonQuery, QueryParser qparser) throws JSONException
  {
    if (jsonQuery == null)
      return null;

    Iterator<String> iter = jsonQuery.keys();
    if (!iter.hasNext())
      return null;

    String type = iter.next();

    QueryConstructor queryConstructor = QueryConstructor.getQueryConstructor(type, qparser);
    if (queryConstructor == null)
      throw new IllegalArgumentException("Query type '" + type + "' not supported");

    return queryConstructor.doConstructQuery(jsonQuery.getJSONObject(type));
  }

  abstract protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException;
}
