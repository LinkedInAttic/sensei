package com.sensei.search.query.filters;

import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
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
	
  private static final Map<String, FilterConstructor> FILTER_CONSTRUCTOR_MAP = 
    new HashMap<String, FilterConstructor>();

  static
  {
    FILTER_CONSTRUCTOR_MAP.put(UIDFilterConstructor.FILTER_TYPE, new UIDFilterConstructor());
    FILTER_CONSTRUCTOR_MAP.put(AndFilterConstructor.FILTER_TYPE, new AndFilterConstructor());
    FILTER_CONSTRUCTOR_MAP.put(FacetSelectionFilterConstructor.FILTER_TYPE, new FacetSelectionFilterConstructor());
    FILTER_CONSTRUCTOR_MAP.put(OrFilterConstructor.FILTER_TYPE, new OrFilterConstructor());
    FILTER_CONSTRUCTOR_MAP.put(RangeFilterConstructor.FILTER_TYPE, new RangeFilterConstructor());
    FILTER_CONSTRUCTOR_MAP.put(TermFilterConstructor.FILTER_TYPE, new TermFilterConstructor());
    FILTER_CONSTRUCTOR_MAP.put(BoolFilterConstructor.FILTER_TYPE, new BoolFilterConstructor());
    FILTER_CONSTRUCTOR_MAP.put(PathFilterConstructor.FILTER_TYPE, new PathFilterConstructor());
    FILTER_CONSTRUCTOR_MAP.put(TermsFilterConstructor.FILTER_TYPE, new TermsFilterConstructor());
  }
  
  public static FilterConstructor getFilterConstructor(String type, Analyzer analyzer)
  {
    FilterConstructor filterConstructor = FILTER_CONSTRUCTOR_MAP.get(type);
    if (filterConstructor == null)
    {
      if (QueryFilterConstructor.FILTER_TYPE.equals(type))
        filterConstructor = new QueryFilterConstructor(analyzer);
    }
    return filterConstructor;
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

	public static Filter constructFilter(JSONObject json, Analyzer analyzer) throws Exception
  {
    if (json == null)
      return null;

    Iterator<String> iter = json.keys();
    if (!iter.hasNext())
      throw new IllegalArgumentException("Filter type not specified: " + json);

    String type = iter.next();

    FilterConstructor filterConstructor = FilterConstructor.getFilterConstructor(type, analyzer);
    if (filterConstructor == null)
      throw new IllegalArgumentException("Filter type '" + type + "' not supported");

    return filterConstructor.doConstructFilter(json.getJSONObject(type));
  }
	
	abstract protected Filter doConstructFilter(JSONObject json) throws Exception;

}
