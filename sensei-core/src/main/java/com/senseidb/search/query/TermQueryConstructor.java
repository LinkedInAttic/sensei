package com.senseidb.search.query;

import java.util.Iterator;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import org.json.JSONException;
import org.json.JSONObject;


public class TermQueryConstructor extends QueryConstructor
{
  public static final String QUERY_TYPE = "term";

  // "term" : {
  //   "color" : "red"
  // 
  //   // or "color" : {"value" : "red", "boost": 2.0}
  // },

  @Override
  protected Query doConstructQuery(JSONObject json) throws JSONException
  {
    Iterator<String> iter = json.keys();
    if (!iter.hasNext())
      throw new IllegalArgumentException("no term value specified: " + json);

    String field = iter.next();

    Object obj = json.get(field);

    String txt;
    float boost;
    if (obj instanceof JSONObject)
    {
      txt = ((JSONObject)obj).optString(TERM_PARAM);
      if (txt == null || txt.length() == 0)
        txt = ((JSONObject)obj).getString(VALUE_PARAM);
      boost = (float)((JSONObject)obj).optDouble(BOOST_PARAM, 1.0);
    }
    else
    {
      txt   = (String)obj;
      boost = 1.0f;
    }
    Query q = new TermQuery(new Term(field, txt));
    q.setBoost(boost);
    return q;
  }
}
