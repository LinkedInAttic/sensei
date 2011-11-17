package com.sensei.search.query.filters;

import org.apache.lucene.search.Filter;
import org.json.JSONArray;
import org.json.JSONObject;

import com.sensei.search.query.FilterConstructor;
import com.sensei.search.query.SenseiTermFilter;
import com.sensei.search.util.RequestConverter2;

public class TermsFilterConstructor extends FilterConstructor{
	@Override
	public Filter constructFilter(JSONObject json) throws Exception {
		boolean noOptimize = json.optBoolean("_noOptimize",false);
		
		String[] names = JSONObject.getNames(json);
		String termName = null;
		for (String name : names){
			if (!name.equals("_noOptimize")){
				termName = name;
				break;
			}
		}
		
		if (termName == null) throw new IllegalArgumentException("no term name specified: "+json);
		
		Object obj = json.opt(termName);
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
			String[] vals = RequestConverter2.getStrings(jsonObj, "values");
			String[] notVals = RequestConverter2.getStrings(jsonObj, "excludes");
			String op = jsonObj.optString("operator","or");
			boolean isAnd = false;
			if (!"or".equals(op)){
				isAnd = true;
			}
			return new SenseiTermFilter(termName, vals, notVals, isAnd, noOptimize);
		}
		else{
			throw new IllegalArgumentException("invalid term value specified: "+json);
		}
	}
}
