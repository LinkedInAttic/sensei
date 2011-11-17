package com.sensei.search.query;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.json.JSONException;
import org.json.JSONObject;

public class QueryStringQueryConstructor extends QueryConstructor {

  public static final String QUERY_TYPE = "query_string";
  
  @Override
  public Query constructQuery(JSONObject params) throws JSONException {
    String defaultField = params.optString("default_field", "_all");
    String queryText = params.getString("query");
    QueryParser qparser = new QueryParser(Version.LUCENE_30,defaultField,new StandardAnalyzer(Version.LUCENE_30));
    try {
      return qparser.parse(queryText);
    } catch (ParseException e) {
      throw new JSONException(e);
    }
  }

}
