package com.sensei.search.query.filters;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.Filter;
import org.json.JSONObject;

public abstract class FilterConstructor {

	public static final String VALUES_PARAM = "values";
	public static final String EXCLUDES_PARAM = "excludes";
	public static final String OPERATOR_PARAM = "operator";
	public static final String PARAMS_PARAM = "params";
	public static final String MUST_PARAM = "must";
	public static final String MUST_NOT_PARAM = "must_not";
	public static final String SHOULD_PARAM = "should";
	
	public static FilterConstructor getFilterConstructor(String type){
		return null;
	}
	
	public static Map<String,String> convertParams(JSONObject obj){
		Map<String,String> paramMap = new HashMap<String,String>();
		String[] names = JSONObject.getNames(obj);
		if (names!=null){
		  for (String name:names){
			String val = obj.optString(name, null);
			if (val!=null){
				paramMap.put(name, val);
			}
		  }
		}
		return paramMap;
	}

	public static Filter constructFilter(JSONObject json) throws Exception
  {
    return null;
  }
	
	abstract protected Filter doConstructFilter(JSONObject json) throws Exception;

}
