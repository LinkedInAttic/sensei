package com.sensei.search.query.filters;

import org.apache.lucene.search.Filter;
import org.json.JSONArray;
import org.json.JSONObject;

import com.sensei.search.util.RequestConverter2;

public class TermsFilterConstructor extends FilterConstructor{
  public static final String FILTER_TYPE = "terms";

  @Override
  protected Filter doConstructFilter(Object obj) throws Exception {
    JSONObject json = (JSONObject)obj;
    boolean noOptimize = json.optBoolean(NOOPTIMIZE_PARAM,false);
    
    String[] names = JSONObject.getNames(json);
    String termName = null;
    for (String name : names){
      if (!name.equals(NOOPTIMIZE_PARAM)){
        termName = name;
        break;
      }
    }
    
    if (termName == null) throw new IllegalArgumentException("no term name specified: "+json);
    
    obj = json.opt(termName);
    if (obj == null){
      throw new IllegalArgumentException("no term value specified: "+json);
    }
    if (obj instanceof JSONArray){
      JSONArray jsonArray = (JSONArray)obj;
      String[] vals = RequestConverter2.getStrings(jsonArray);
      return new SenseiTermFilter(termName, vals, null, false, noOptimize);  
    }
    else if (obj instanceof JSONObject){
      JSONObject jsonObj = (JSONObject)obj;
      String[] vals = RequestConverter2.getStrings(jsonObj, VALUES_PARAM);
      String[] notVals = RequestConverter2.getStrings(jsonObj, EXCLUDES_PARAM);
      String op = jsonObj.optString(OPERATOR_PARAM, OR_PARAM);
      boolean isAnd = false;
      if (!OR_PARAM.equals(op)){
        isAnd = true;
      }
      return new SenseiTermFilter(termName, vals, notVals, isAnd, noOptimize);
    }
    else{
      throw new IllegalArgumentException("invalid term value specified: "+json);
    }
  }
}
