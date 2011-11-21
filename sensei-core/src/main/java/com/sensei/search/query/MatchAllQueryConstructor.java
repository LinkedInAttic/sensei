package com.sensei.search.query;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.json.JSONException;
import org.json.JSONObject;

public class MatchAllQueryConstructor extends QueryConstructor{

  public static final String QUERY_TYPE = "match_all";
  
  @Override
  protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException
  {
    double boost = jsonQuery.optDouble("boost",1.0);
    
    MatchAllDocsQuery q = new MatchAllDocsQuery();
    q.setBoost((float)boost);
    
    return q;
  }
  
}
