package com.sensei.search.query;

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

  // "terms" : { // TODO
  //     "tags" : {
  //         "values" : [ "blue", "pill" ],
  //         "excludes" : ["red"],
  //         "minimum_match" : 1,
  //         "operator" : "or"}
  //     // or
  //     // "tags" : [ "blue", "pill" ], default operator or
  // },

  @Override
  protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException
  {
    String field = null;
    JSONArray values = null, excludes = null;
    String op = null;
    int minimum_match = 1;
    float boost = 1.0f;

    for (String name : JSONObject.getNames(jsonQuery))
    {
      if (OPERATOR_PARAM.equals(name))
        op = jsonQuery.getString(name);
      else if (MINIMUM_MATCH_PARAM.equals(name))
        minimum_match = jsonQuery.getInt(name);
      else if (BOOST_PARAM.equals(name))
        boost = (float)jsonQuery.getDouble(name);
      else
        field = name;
    }
    if (field == null)
      throw new IllegalArgumentException("no term value specified: " + jsonQuery);

    Object obj = jsonQuery.get(field);
    if (obj instanceof JSONObject)
    {
      values          = ((JSONObject)obj).optJSONArray(VALUES_PARAM);
      excludes        = ((JSONObject)obj).optJSONArray(EXCLUDES_PARAM);
      op              = ((JSONObject)obj).optString(OPERATOR_PARAM);
      minimum_match   = ((JSONObject)obj).optInt(MINIMUM_MATCH_PARAM, 1);
    }
    else if (obj instanceof JSONArray)
    {
      values = (JSONArray)obj;
    }

    BooleanQuery query = new BooleanQuery();
    if (values != null)
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
    }
    if (excludes != null)
    {
      for (int i=0; i<excludes.length(); ++i)
      {
        query.add(new TermQuery(new Term(field, values.getString(i))), BooleanClause.Occur.MUST_NOT);
      }
    }

    query.setMinimumNumberShouldMatch(minimum_match);
    query.setBoost(boost);

    return query;
  }
}

