package com.sensei.search.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.json.JSONObject;

public abstract class QueryConstructor
{
  abstract public Query constructQuery(JSONObject params);
  
  public static QueryConstructor getQueryConstructor(String type)
  {
    if ("match_all".equals(type))
    {
      return new MatchAllQueryConstructor();
    }
    else if ("ids".equals(type))
    {
    }
    else if ("query_string".equals(type))
    {
    }
    else if ("text".equals(type))
    {
      return new TextQueryConstructor();
    }
    else if ("term".equals(type))
    {
      return new TermQueryConstructor();
    }
    else if ("bool".equals(type))
    {
    }
    else if ("dis_max".equals(type))
    {
    }
    else if ("prefix".equals(type))
    {
    }
    else if ("wildcard".equals(type))
    {
    }
    else if ("range".equals(type))
    {
    }
    else if ("filtered".equals(type))
    {
    }
    else if ("terms".equals(type))
    {
    }
    else if ("span_term".equals(type))
    {
    }
    else if ("span_or".equals(type))
    {
    }
    else if ("span_not".equals(type))
    {
    }
    else if ("span_near".equals(type))
    {
    }
    else if ("span_first".equals(type))
    {
    }
    else if ("custom".equals(type))
    {
    }

    throw new IllegalArgumentException("query type: " + type);
  }
  
  public static class TermQueryConstructor extends QueryConstructor
  {
    @Override
    public Query constructQuery(JSONObject params)
    {
      if (params == null)
        throw new IllegalArgumentException("no term specified: " + params);

      for (String field : JSONObject.getNames(params))
      {
        Object obj = params.opt(field);
        String txt;
        float boost;
        if (obj == null)
          throw new IllegalArgumentException("no term value specified: " + params);
        else if (obj instanceof JSONObject)
        {
          txt = ((JSONObject)obj).optString("term");
          if (txt == null || txt.length() == 0)
            txt = ((JSONObject)obj).optString("value");
          if (txt == null || txt.length() == 0)
            throw new IllegalArgumentException("no term value specified: " + params);
          boost = (float)((JSONObject)obj).optDouble("boost", 1.0);
        }
        else
        {
          txt   = String.valueOf(obj);
          boost = 1.0f;
        }
        Query q = new TermQuery(new Term(field, txt));
        q.setBoost(boost);
        return q; // Term query have only one field.
      }

      throw new IllegalArgumentException("no term specified: " + params);
    }
  }
  
  public static class MatchAllQueryConstructor extends QueryConstructor
  {
    @Override
    public Query constructQuery(JSONObject params) {
      double boost = params.optDouble("boost",1.0);
      
      MatchAllDocsQuery q = new MatchAllDocsQuery();
      q.setBoost((float)boost);
      
      return q;
    }
  }
  
  public static class TextQueryConstructor extends QueryConstructor
  {
    @Override
    public Query constructQuery(JSONObject params)
    {
      // TODO Auto-generated method stub
      return null;
    }
  }
}
