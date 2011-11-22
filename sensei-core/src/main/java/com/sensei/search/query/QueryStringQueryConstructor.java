package com.sensei.search.query;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.json.JSONException;
import org.json.JSONObject;

public class QueryStringQueryConstructor extends QueryConstructor {

  public static final String QUERY_TYPE = "query_string";

  private QueryParser _qparser;

  public QueryStringQueryConstructor(QueryParser qparser)
  {
    _qparser = qparser;
  }
  
  @Override
  protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException
  {
    String queryText = jsonQuery.getString("query");
    try
    {
      synchronized(_qparser)
      {
        return _qparser.parse(queryText);
      }
    }
    catch (ParseException e) {
      throw new JSONException(e);
    }
  }

}
