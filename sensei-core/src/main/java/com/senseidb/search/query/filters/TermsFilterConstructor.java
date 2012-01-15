package com.senseidb.search.query.filters;

import java.util.Iterator;

import org.apache.lucene.search.Filter;
import org.json.JSONArray;
import org.json.JSONObject;

import com.senseidb.util.RequestConverter2;

public class TermsFilterConstructor extends FilterConstructor{
  public static final String FILTER_TYPE = "terms";

  @Override
  protected Filter doConstructFilter(Object obj) throws Exception {
    JSONObject json = (JSONObject)obj;

    Iterator<String> iter = json.keys();
    if (!iter.hasNext())
      return null;

    String field = iter.next();

    boolean noOptimize = false;
    
    obj = json.get(field);
    if (obj == null){
      throw new IllegalArgumentException("no term value specified: "+json);
    }
    if (obj instanceof JSONArray){
      JSONArray jsonArray = (JSONArray)obj;
      String[] vals = RequestConverter2.getStrings(jsonArray);
      return new SenseiTermFilter(field, vals, null, false, noOptimize);  
    }
    else if (obj instanceof JSONObject){
      JSONObject jsonObj = (JSONObject)obj;
      String[] vals = RequestConverter2.getStrings(jsonObj, VALUES_PARAM);
      String[] notVals = RequestConverter2.getStrings(jsonObj, EXCLUDES_PARAM);
      String op = jsonObj.optString(OPERATOR_PARAM, OR_PARAM);
      noOptimize = jsonObj.optBoolean(NOOPTIMIZE_PARAM, false);
      boolean isAnd = false;
      if (!OR_PARAM.equals(op)){
        isAnd = true;
      }
      return new SenseiTermFilter(field, vals, notVals, isAnd, noOptimize);
    }
    else{
      throw new IllegalArgumentException("invalid term value specified: "+json);
    }
  }
}
