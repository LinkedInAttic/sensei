package com.sensei.search.query;

import org.apache.lucene.queryParser.QueryParser;
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

  private QueryParser _qparser;

  public BooleanQueryConstructor(QueryParser qparser)
  {
    _qparser = qparser;
  }

  @Override
  protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException
  {
    BooleanQuery query = new BooleanQuery(jsonQuery.optBoolean("disable_coord", false));
    JSONObject obj = jsonQuery.optJSONObject("must");
    if (obj != null)
    {
      query.add(QueryConstructor.constructQuery(obj, _qparser), BooleanClause.Occur.MUST);
    }
    obj = jsonQuery.optJSONObject("must_not");
    if (obj != null)
    {
      query.add(QueryConstructor.constructQuery(obj, _qparser), BooleanClause.Occur.MUST_NOT);
    }
    JSONArray array = jsonQuery.optJSONArray("should");
    if (array != null)
    {
      for (int i=0; i<array.length(); ++i)
      {
        obj = array.getJSONObject(i);
        query.add(QueryConstructor.constructQuery(obj, _qparser), BooleanClause.Occur.SHOULD);
      }
    }

    query.setMinimumNumberShouldMatch(jsonQuery.optInt("minimum_number_should_match", 1));
    query.setBoost((float)jsonQuery.optDouble("boost", 1.0));

    return query;
  }
}
