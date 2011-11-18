package com.sensei.search.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class BooleanQueryConstructor extends QueryConstructor
{
  public static final String QUERY_TYPE = "bool";

  // "bool" : {
  //     "must" : {
  //         "term" : { "color" : "red" }
  //     },
  //     "must_not" : {
  //         "range" : {
  //             "age" : { "from" : 10, "to" : 20 }
  //         }
  //     },
  //     "should" : [
  //         {
  //             "term" : { "tag" : "wow", "noOptimize" : false}
  //         },
  //         {
  //             "term" : { "tag" : "search"}
  //         }
  //     ],
  //     "minimum_number_should_match" : 1,
  //     "boost" : 1.0,
  //     "disable_coord" : false                      // optional: default = false
  // },

  private Analyzer _analyzer;

  public BooleanQueryConstructor(Analyzer analyzer)
  {
    _analyzer = analyzer;
  }

  @Override
  public Query constructQuery(JSONObject jsonQuery) throws JSONException
  {
    BooleanQuery query = new BooleanQuery(jsonQuery.optBoolean("disable_coord", false));
    JSONObject obj = jsonQuery.optJSONObject("must");
    if (obj != null)
    {
      String type = (String)obj.keys().next();
      QueryConstructor qc = QueryConstructor.getQueryConstructor(type, _analyzer);
      if (qc == null)
        throw new JSONException("Wrong type: " + type);
      query.add(qc.constructQuery(obj), BooleanClause.Occur.MUST);
    }
    obj = jsonQuery.optJSONObject("must_not");
    if (obj != null)
    {
      String type = (String)obj.keys().next();
      QueryConstructor qc = QueryConstructor.getQueryConstructor(type, _analyzer);
      if (qc == null)
        throw new JSONException("Wrong type: " + type);
      query.add(qc.constructQuery(obj), BooleanClause.Occur.MUST_NOT);
    }
    JSONArray array = jsonQuery.optJSONArray("should");
    if (array != null)
    {
      for (int i=0; i<array.length(); ++i)
      {
        obj = array.getJSONObject(i);
        String type = (String)obj.keys().next();
        QueryConstructor qc = QueryConstructor.getQueryConstructor(type, _analyzer);
        if (qc == null)
          throw new JSONException("Wrong type: " + type);
        query.add(qc.constructQuery(obj), BooleanClause.Occur.SHOULD);
      }
    }

    query.setMinimumNumberShouldMatch(jsonQuery.optInt("minimum_number_should_match", 1));
    query.setBoost((float)jsonQuery.optDouble("boost", 1.0));

    return query;
  }
}
