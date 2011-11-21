package com.sensei.search.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.json.JSONException;
import org.json.JSONObject;

public class QueryStringQueryConstructor extends QueryConstructor {

  public static final String QUERY_TYPE = "query_string";

  private Analyzer _analyzer;

  public QueryStringQueryConstructor(Analyzer analyzer)
  {
    _analyzer = analyzer;
  }
  
  @Override
  protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException
  {
    String defaultField = jsonQuery.optString("default_field", "_all");
    String queryText = jsonQuery.getString("query");
    QueryParser qparser = new QueryParser(Version.LUCENE_30, defaultField, _analyzer);
    try {
      return qparser.parse(queryText);
    } catch (ParseException e) {
      throw new JSONException(e);
    }
  }

}
