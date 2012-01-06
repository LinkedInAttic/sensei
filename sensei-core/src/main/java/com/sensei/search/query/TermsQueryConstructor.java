package com.sensei.search.query;

import java.util.Iterator;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class TermsQueryConstructor extends QueryConstructor
{
  public static final String QUERY_TYPE = "terms";

  // "terms" : {
  //     "tags" : {
  //         "values" : [ "blue", "pill" ],
  //         "excludes" : ["red"],
  //         "minimum_match" : 1,
  //         "operator" : "or"}
  //     // or
  //     // "tags" : [ "blue", "pill" ], default operator or
  // },

  @Override
  protected Query doConstructQuery(JSONObject json) throws JSONException
  {
    String field = null;
    JSONArray values = null, excludes = null;
    String op = null;
    int minimum_match = 1;
    float boost = 1.0f;

    Iterator<String> iter = json.keys();
    if (!iter.hasNext())
      throw new IllegalArgumentException("no term value specified: " + json);

    field = iter.next();

    Object obj = json.get(field);
    if (obj instanceof JSONObject)
    {
      values          = ((JSONObject)obj).optJSONArray(VALUES_PARAM);
      excludes        = ((JSONObject)obj).optJSONArray(EXCLUDES_PARAM);
      op              = ((JSONObject)obj).optString(OPERATOR_PARAM);
      minimum_match   = ((JSONObject)obj).optInt(MINIMUM_MATCH_PARAM, 0);
    }
    else if (obj instanceof JSONArray)
    {
      values = (JSONArray)obj;
    }

    BooleanQuery query = new BooleanQuery();
    if (values != null && values.length() > 0)
    {
      for (int i=0; i<values.length(); ++i)
      {
        if (AND_PARAM.equals(op))
        {
          query.add(new TermQuery(new Term(field, values.getString(i))), BooleanClause.Occur.MUST);
        }
        else
        {
          query.add(new TermQuery(new Term(field, values.getString(i))), BooleanClause.Occur.SHOULD);
        }
      }
      if (!AND_PARAM.equals(op))
        query.setMinimumNumberShouldMatch(minimum_match);
    }
    if (excludes != null)
    {
      for (int i=0; i<excludes.length(); ++i)
      {
        query.add(new TermQuery(new Term(field, excludes.getString(i))), BooleanClause.Occur.MUST_NOT);
      }
    }

    query.setBoost(boost);

    return query;
  }
}

