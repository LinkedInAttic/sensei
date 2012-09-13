package com.senseidb.search.query.filters;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Filter;
import org.json.JSONObject;

public abstract class FilterConstructor {

  public static final String VALUES_PARAM        = "values";
  public static final String VALUE_PARAM         = "value";
  public static final String EXCLUDES_PARAM      = "excludes";
  public static final String OPERATOR_PARAM      = "operator";
  public static final String PARAMS_PARAM        = "params";
  public static final String MUST_PARAM          = "must";
  public static final String MUST_NOT_PARAM      = "must_not";
  public static final String SHOULD_PARAM        = "should";
  public static final String FROM_PARAM          = "from";
  public static final String TO_PARAM            = "to";
  public static final String NOOPTIMIZE_PARAM    = "_noOptimize";
  public static final String RANGE_FIELD_TYPE    = "_type";
  public static final String RANGE_DATE_FORMAT   = "_date_format";
  public static final String QUERY_PARAM         = "query";
  public static final String OR_PARAM            = "or";
  public static final String DEPTH_PARAM         = "depth";
  public static final String STRICT_PARAM        = "strict";
  public static final String INCLUDE_LOWER_PARAM = "include_lower";
  public static final String INCLUDE_UPPER_PARAM = "include_upper";
  public static final String GT_PARAM            = "gt";
  public static final String GTE_PARAM           = "gte";
  public static final String LT_PARAM            = "lt";
  public static final String LTE_PARAM           = "lte";
  public static final String CLASS_PARAM         = "class";  
	
  private static final Map<String, FilterConstructor> FILTER_CONSTRUCTOR_MAP = 
    new HashMap<String, FilterConstructor>();

  static
  {
    FILTER_CONSTRUCTOR_MAP.put(UIDFilterConstructor.FILTER_TYPE, new UIDFilterConstructor());
    FILTER_CONSTRUCTOR_MAP.put(FacetSelectionFilterConstructor.FILTER_TYPE, new FacetSelectionFilterConstructor());
    FILTER_CONSTRUCTOR_MAP.put(RangeFilterConstructor.FILTER_TYPE, new RangeFilterConstructor());
    FILTER_CONSTRUCTOR_MAP.put(TermFilterConstructor.FILTER_TYPE, new TermFilterConstructor());
    FILTER_CONSTRUCTOR_MAP.put(PathFilterConstructor.FILTER_TYPE, new PathFilterConstructor());
    FILTER_CONSTRUCTOR_MAP.put(TermsFilterConstructor.FILTER_TYPE, new TermsFilterConstructor());
    FILTER_CONSTRUCTOR_MAP.put(NullFilterConstructor.FILTER_TYPE, new NullFilterConstructor());
    FILTER_CONSTRUCTOR_MAP.put(CustomFilterConstructor.FILTER_TYPE, new CustomFilterConstructor());
    FILTER_CONSTRUCTOR_MAP.put(ConstExpFilterConstructor.FILTER_TYPE, new ConstExpFilterConstructor());
  }
  
  public static FilterConstructor getFilterConstructor(String type, QueryParser qparser)
  {
    FilterConstructor filterConstructor = FILTER_CONSTRUCTOR_MAP.get(type);
    if (filterConstructor == null)
    {
      if (QueryFilterConstructor.FILTER_TYPE.equals(type))
        filterConstructor = new QueryFilterConstructor(qparser);
      else if (AndFilterConstructor.FILTER_TYPE.equals(type))
        filterConstructor = new AndFilterConstructor(qparser);
      else if (OrFilterConstructor.FILTER_TYPE.equals(type))
        filterConstructor = new OrFilterConstructor(qparser);
      else if (BooleanFilterConstructor.FILTER_TYPE.equals(type))
        filterConstructor = new BooleanFilterConstructor(qparser);
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

	public static Filter constructFilter(JSONObject json, QueryParser qparser) throws Exception
  {
    if (json == null)
      return null;

    Iterator<String> iter = json.keys();
    if (!iter.hasNext())
      throw new IllegalArgumentException("Filter type not specified: " + json);

    String type = iter.next();

    FilterConstructor filterConstructor = FilterConstructor.getFilterConstructor(type, qparser);
    if (filterConstructor == null)
      throw new IllegalArgumentException("Filter type '" + type + "' not supported");

    return filterConstructor.doConstructFilter(json.get(type));
  }
	
	abstract protected Filter doConstructFilter(Object json/* JSONObject or JSONArray */) throws Exception;

}
