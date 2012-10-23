package com.senseidb.util;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.lucene.search.SortField;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.BrowseSelection.ValueOperation;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.DefaultFacetHandlerInitializerParam;
import com.browseengine.bobo.facets.impl.PathFacetHandler;

import com.senseidb.indexing.DefaultSenseiInterpreter;
import com.senseidb.indexing.MetaType;
import com.senseidb.search.req.SenseiJSONQuery;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.search.req.mapred.impl.MapReduceRegistry;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;


public class RequestConverter2 {

  public static final String PAGING_SIZE = "size";
  public static final String PAGING_FROM = "from";

  public static final String GROUPBY = "groupBy";
  public static final String GROUPBY_COLUMN = "column";
  public static final String GROUPBY_TOP = "top";

  public static final String SELECTIONS = "selections";
  public static final String SELECTIONS_TERM = "term";
  public static final String SELECTIONS_TERM_VALUE = "value";
  public static final String SELECTIONS_TERMS = "terms";
  public static final String SELECTIONS_TERMS_VALUES = "values";
  public static final String SELECTIONS_TERMS_EXCLUDES = "excludes";
  public static final String SELECTIONS_TERMS_OPERATOR = "operator";
  public static final String SELECTIONS_TERMS_OPERATOR_OR = "or";
  public static final String SELECTIONS_TERMS_OPERATOR_AND = "and";
  public static final String SELECTIONS_RANGE = "range";
  public static final String SELECTIONS_RANGE_FROM = "from";
  public static final String SELECTIONS_RANGE_TO = "to";
  public static final String SELECTIONS_RANGE_INCLUDE_LOWER = "include_lower";
  public static final String SELECTIONS_RANGE_INCLUDE_UPPER = "include_upper";
  public static final String SELECTIONS_PATH = "path";
  public static final String SELECTIONS_PATH_VALUE = "value";
  public static final String SELECTIONS_PATH_STRICT = "strict";
  public static final String SELECTIONS_PATH_DEPTH = "depth";
  public static final String SELECTIONS_CUSTOM = "custom";
  public static final String SELECTIONS_DEFAULT = "default";

  public static final String FACETS = "facets";
  public static final String FACETS_MAX = "max";
  public static final String FACETS_MINCOUNT = "minCount";
  public static final String FACETS_EXPAND = "expand";
  public static final String FACETS_ORDER = "order";
  public static final String FACETS_ORDER_HITS = "hits";
  public static final String FACETS_ORDER_VAL = "val";

  public static final String FACETINIT = "facetInit";
  public static final String FACETINIT_TYPE = "type";
  public static final String FACETINIT_TYPE_INT = "int";
  public static final String FACETINIT_TYPE_STRING = "string";
  public static final String FACETINIT_TYPE_BOOLEAN = "boolean";
  public static final String FACETINIT_TYPE_LONG = "long";
  public static final String FACETINIT_TYPE_BYTES = "bytes";
  public static final String FACETINIT_TYPE_DOUBLE = "double";
  public static final String FACETINIT_VALUES = "values";


  public static final String SORT = "sort";
  public static final String SORT_ASC = "asc";
  public static final String SORT_DESC = "desc";
  public static final String SORT_SCORE = "_score";
  public static final String SORT_RELEVANCE = "RELEVANCE";

  public static final String FETCH_STORED = "fetchStored";

  public static final String FETCH_STORED_VALUE = "fetchStoredValue";

  public static final String TERM_VECTORS = "termVectors";

  public static final String PARTITIONS = "partitions";

  public static final String EXPLAIN = "explain";

  public static final String ROUTEPARAM = "routeParam";

  public static final String MAPPINGS = "mappings";
  private static final String MAP_REDUCE = "mapReduce";
  private static final String MAP_REDUCE_FUNCTION = "function";
  private static final String MAP_REDUCE_PARAMETERS = "parameters";

  private static JsonTemplateProcessor jsonTemplateProcessor = new JsonTemplateProcessor();

	public static String[] getStrings(JSONObject obj,String field){
		  String[] strArray = null;
		  JSONArray array = obj.optJSONArray(field);
		  if (array!=null){
			int count = array.length();
			strArray = new String[count];
			for (int i=0;i<count;++i){
				strArray[i] = array.optString(i);
			}
		  }
		  return strArray;
	  }

	  private static int[] getInts(JSONObject obj,String field,int defaultVal){
		  int[] intArray = null;
		  JSONArray array = obj.optJSONArray(field);
		  if (array!=null){
			int count = array.length();
			intArray = new int[count];
			for (int i=0;i<count;++i){
				intArray[i] = array.optInt(i,defaultVal);
			}
		  }
		  return intArray;
	  }

	  private static Set<Integer> getIntSet(JSONObject obj,String field,int defaultVal){
		  HashSet<Integer> intSet = null;
		  JSONArray array = obj.optJSONArray(field);
		  if (array!=null){
			int count = array.length();
			intSet = new HashSet<Integer>(count);
			for (int i=0;i<count;++i){
				intSet.add(array.optInt(i,defaultVal));
			}
		  }
		  return intSet;
	  }

	  public static String[] getStrings(JSONArray jsonArray) throws Exception{
      if (jsonArray == null)
        return null;
		  int count = jsonArray.length();
		  String[] vals = new String[count];
		  for (int i=0;i<count;++i){
			vals[i] = jsonArray.getString(i);
		  }
		  return vals;
	  }


  /**
   * Builds SenseiRequest based on a JSON object.
   *
   * @param json  The input JSON object.
   * @return The built SenseiRequest.
   */
  public static SenseiRequest fromJSON(JSONObject json)
    throws Exception
  {
    return fromJSON(json, null);
  }


  /**
   * Builds SenseiRequest based on a JSON object.
   *
   * @param json  The input JSON object.
   * @param facetInfoMap  Facet information map, which maps a facet name
   *        to a String array in which the first element is the facet
   *        type (like "simple" or "range") and the second element is
   *        the data type (like "int" or "long").
   * @return The built SenseiRequest.
   */
  public static SenseiRequest fromJSON(JSONObject json,
                                       final Map<String, String[]> facetInfoMap)
    throws Exception
  {
	  json = jsonTemplateProcessor.substituteTemplates(json);

	  SenseiRequest req = new SenseiRequest();

    JSONObject meta = json.optJSONObject("meta");
    if (meta != null)
    {
      JSONArray array = meta.optJSONArray("select_list");
      if (array != null)
      {
        List<String> list = new ArrayList<String>();
        for (int i = 0; i < array.length(); ++i)
        {
          list.add(array.get(i).toString());
        }
        req.setSelectList(list);
      }
    }

	    // query
	    req.setQuery(new SenseiJSONQuery(json));

		// paging params

	    int  count = json.optInt(RequestConverter2.PAGING_SIZE, 10);
	    int  offset = json.optInt(RequestConverter2.PAGING_FROM, 0);
	    req.setCount(count);
	    req.setOffset(offset);


        // group by
        JSONObject groupBy = json.optJSONObject("groupBy");
        if (groupBy != null)
        {
          JSONArray columns = groupBy.optJSONArray("columns");
          if (columns != null && columns.length() >= 1)
          {
            String[] groupByArray = new String[columns.length()];
            for (int i=0; i<columns.length(); ++i)
              groupByArray[i] = columns.getString(i);
            req.setGroupBy(groupByArray);
          }
          req.setMaxPerGroup(groupBy.optInt("top", groupBy.optInt("count", 1)));
        }

      // distinct
      JSONObject distinct = json.optJSONObject("distinct");
      if (distinct != null)
      {
        JSONArray columns = distinct.optJSONArray("columns");
        if (columns != null && columns.length() >= 1)
        {
          String[] distinctArray = new String[columns.length()];
          for (int i=0; i<columns.length(); ++i)
            distinctArray[i] = columns.getString(i);
          if (distinctArray.length == 1 && req.getGroupBy() == null)
          {
            // rewrite to use group by
            req.setGroupBy(distinctArray);
            req.setMaxPerGroup(0);
          }
          else
          {
            req.setDistinct(distinctArray);
          }
        }
      }

      // selections
      Object selections = json.opt(RequestConverter2.SELECTIONS);
      if (selections == null)
      {
        // ignore
      }
      else if (selections instanceof JSONArray)
      {
        JSONArray selectionArray = (JSONArray)selections;
        for(int i=0; i<selectionArray.length(); i++)
        {
          JSONObject selItem = selectionArray.optJSONObject(i);
          if(selItem != null){
            Iterator<String> keyIter = selItem.keys();
            while(keyIter.hasNext()){
              String type = keyIter.next();
              JSONObject jsonSel = selItem.optJSONObject(type);
              if(jsonSel != null){
                addSelection(type, jsonSel, req, facetInfoMap);
              }
            }
          }
        }
      }
      else if (selections instanceof JSONObject)
      {
        JSONObject selectionObject = (JSONObject)selections;
        Iterator<String> keyIter = selectionObject.keys();
        while (keyIter.hasNext())
        {
          String type = keyIter.next();
          JSONObject jsonSel = selectionObject.optJSONObject(type);
          if (jsonSel != null)
            addSelection(type, jsonSel, req, facetInfoMap);
        }
      }
      //map reduce
      JSONObject mapReduceJson =  json.optJSONObject(RequestConverter2.MAP_REDUCE);
      if (mapReduceJson != null) {
        String key = mapReduceJson.getString(MAP_REDUCE_FUNCTION);
        SenseiMapReduce senseiMapReduce = MapReduceRegistry.get(key);
        senseiMapReduce.init(mapReduceJson.optJSONObject(MAP_REDUCE_PARAMETERS));
        req.setMapReduceFunction(senseiMapReduce);
      }
		 // facets
		  JSONObject facets = json.optJSONObject(RequestConverter2.FACETS);
		  if (facets!=null){
			  Iterator<String> keyIter = facets.keys();
			  while (keyIter.hasNext()){
				  String field = keyIter.next();
				  JSONObject facetObj = facets.getJSONObject(field);
				  if (facetObj!=null){
					 FacetSpec facetSpec = new FacetSpec();
					 facetSpec.setMaxCount(facetObj.optInt(RequestConverter2.FACETS_MAX, 10));
					 facetSpec.setMinHitCount(facetObj.optInt(RequestConverter2.FACETS_MINCOUNT, 1));
					 facetSpec.setExpandSelection(facetObj.optBoolean(RequestConverter2.FACETS_EXPAND, false));

					 String orderBy = facetObj.optString(RequestConverter2.FACETS_ORDER, RequestConverter2.FACETS_ORDER_HITS);
					 FacetSpec.FacetSortSpec facetOrder = FacetSpec.FacetSortSpec.OrderHitsDesc;
					 if (RequestConverter2.FACETS_ORDER_VAL.equals(orderBy)){
						 facetOrder = FacetSpec.FacetSortSpec.OrderValueAsc;
					 }
					 facetSpec.setProperties(createFacetProperties(facetObj));
					 facetSpec.setOrderBy(facetOrder);
					 req.setFacetSpec(field, facetSpec);
				  }
			  }
		  }

		  //facet init;
          JSONObject facetInitParams = json.optJSONObject(RequestConverter2.FACETINIT);
          if (facetInitParams != null)
          {
            Iterator<String> keyIter = facetInitParams.keys();
            while (keyIter.hasNext())
            {
              // may have multiple facets;
              String facetName = keyIter.next();
              DefaultFacetHandlerInitializerParam param =
                  new DefaultFacetHandlerInitializerParam();

              JSONObject jsonParams = facetInitParams.getJSONObject(facetName);
              if (jsonParams != null && jsonParams.length() > 0)
              {
                Iterator<String> paramIter = jsonParams.keys();
                while (paramIter.hasNext())
                {
                  // each facet may have multiple parameters to be configured;
                  String paramName = paramIter.next();
                  JSONObject jsonParamValues = jsonParams.getJSONObject(paramName);
                  String type = jsonParamValues.optString(RequestConverter2.FACETINIT_TYPE, RequestConverter2.FACETINIT_TYPE_STRING);
                  JSONArray jsonValues = jsonParamValues.optJSONArray(RequestConverter2.FACETINIT_VALUES);
                  if (jsonValues == null)
                  {
                    // Accept scalar values here too.  This is useful in
                    // supporting variable substitutions.
                    Object value = jsonParamValues.opt(RequestConverter2.FACETINIT_VALUES);
                    if (value != null)
                    {
                      jsonValues = new FastJSONArray().put(value);
                    }
                  }
                  if (jsonValues != null)
                  {
                    if (type.equals(RequestConverter2.FACETINIT_TYPE_INT))
                      param.putIntParam(paramName, convertJSONToIntArray(jsonValues));
                    else if (type.equals(RequestConverter2.FACETINIT_TYPE_STRING))
                      param.putStringParam(paramName, convertJSONToStringArray(jsonValues));
                    else if (type.equals(RequestConverter2.FACETINIT_TYPE_BOOLEAN))
                      param.putBooleanParam(paramName, convertJSONToBoolArray(jsonValues));
                    else if (type.equals(RequestConverter2.FACETINIT_TYPE_LONG))
                      param.putLongParam(paramName, convertJSONToLongArray(jsonValues));
                    else if (type.equals(RequestConverter2.FACETINIT_TYPE_BYTES))
                      param.putByteArrayParam(paramName, convertJSONToByteArray(jsonValues));
                    else if (type.equals(RequestConverter2.FACETINIT_TYPE_DOUBLE))
                      param.putDoubleParam(paramName, convertJSONToDoubleArray(jsonValues));
                  }
                }
                req.setFacetHandlerInitializerParam(facetName, param);
              }

            }
          }

		// sorts

          JSONArray sortArray = json.optJSONArray(RequestConverter2.SORT);
          if (sortArray!=null && sortArray.length()>0){
            ArrayList<SortField> sortFieldList = new ArrayList<SortField>(sortArray.length());
            for (int i=0;i<sortArray.length();++i){
              Object obj = sortArray.opt(i);
              if(obj instanceof JSONObject){
                String field = (String) ((JSONObject)obj).keys().next();
                if (field == null || field.length() == 0)
                  continue;
                if (SORT_SCORE.equals(field) || SORT_RELEVANCE.equalsIgnoreCase(field))
                {
                  sortFieldList.add(SortField.FIELD_SCORE);
                  continue;
                }
                String order = ((JSONObject)obj).optString(field);
                boolean rev = false;
                if(RequestConverter2.SORT_DESC.equals(order))
                  rev = true;
                sortFieldList.add(new SortField(field,SortField.CUSTOM,rev));
                continue;
              }
              else if (obj instanceof String){
                if(SORT_SCORE.equals(obj)){
                  sortFieldList.add(SortField.FIELD_SCORE);
                  continue;
                }
              }
            }


            if (sortFieldList.size()>0){
              req.setSort(sortFieldList.toArray(new SortField[sortFieldList.size()]));
            }
          }

		// other

		boolean fetchStored = json.optBoolean(RequestConverter2.FETCH_STORED);
		req.setFetchStoredFields(fetchStored);

		boolean fetchStoredValue = json.optBoolean(RequestConverter2.FETCH_STORED_VALUE);
		req.setFetchStoredValue(fetchStoredValue);

		String[] termVectors = getStrings(json,RequestConverter2.TERM_VECTORS);
		if (termVectors!=null && termVectors.length>0){
		  req.setTermVectorsToFetch(new HashSet<String>(Arrays.asList(termVectors)));
		}


		req.setPartitions(getIntSet(json, RequestConverter2.PARTITIONS,0));

		req.setShowExplanation(json.optBoolean(RequestConverter2.EXPLAIN,false));

		String routeParam = json.optString(RequestConverter2.ROUTEPARAM,null);
		req.setRouteParam(routeParam);

		return req;
	}

  private static String[] formatValues(String facet,
                                       String[] values,
                                       final Map<String, String[]> facetInfoMap)
  {
    String[] facetInfo = facetInfoMap == null ? null : facetInfoMap.get(facet);
    if (facetInfo != null &&
        ("simple".equals(facetInfo[0]) || "multi".equals(facetInfo[0])))
    {
      MetaType metaType = null;
      String formatString = null;
      DecimalFormat formatter = null;
      String type = facetInfo[1];

      if ("int".equals(type))
      {
        metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(int.class);
        formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
        formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
        for (int i = 0; i < values.length; ++i)
        {
          values[i] = formatter.format(Integer.parseInt(values[i]));
        }
      }
      else if ("short".equals(type))
      {
        metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(short.class);
        formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
        formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
        for (int i = 0; i < values.length; ++i)
        {
          values[i] = formatter.format(Short.parseShort(values[i]));
        }
      }
      else if ("long".equals(type))
      {
        metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(long.class);
        formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
        formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
        for (int i = 0; i < values.length; ++i)
        {
          values[i] = formatter.format(Long.parseLong(values[i]));
        }
      }
      else if ("float".equals(type)) {
        metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(float.class);
        formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
        formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
        for (int i = 0; i < values.length; ++i)
        {
          values[i] = formatter.format(Float.parseFloat(values[i]));
        }
      }
      else if ("double".equals(type)) {
        metaType = DefaultSenseiInterpreter.CLASS_METATYPE_MAP.get(double.class);
        formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(metaType);
        formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
        for (int i = 0; i < values.length; ++i)
        {
          values[i] = formatter.format(Double.parseDouble(values[i]));
        }
      }
    }
    return values;
  }

  private static void addSelection(String type,
                                   JSONObject jsonSel,
                                   SenseiRequest req,
                                   final Map<String, String[]> facetInfoMap)
    throws Exception
  {
    // we process "term", "terms", "range", "path", "custom" selection types;

    if(RequestConverter2.SELECTIONS_TERM.equals(type))
    {
      Iterator<String> iter = jsonSel.keys();
      if(iter.hasNext()){
        String facet = iter.next();
        JSONObject jsonParams = jsonSel.optJSONObject(facet);
        String value = jsonParams.optString(RequestConverter2.SELECTIONS_TERM_VALUE, null);
        if(facet!= null && value != null)
        {
          BrowseSelection sel = new BrowseSelection(facet);
          String[] vals = new String[1];
          vals[0] = value;
          sel.setValues(formatValues(facet, vals, facetInfoMap));
          req.addSelection(sel);
        }
      }
    }
    else if(RequestConverter2.SELECTIONS_TERMS.equals(type))
    {
      Iterator<String> iter = jsonSel.keys();
      if(iter.hasNext()){
        String facet = iter.next();
        JSONObject jsonParams = jsonSel.optJSONObject(facet);
        JSONArray values = jsonParams.optJSONArray(RequestConverter2.SELECTIONS_TERMS_VALUES);
        JSONArray excludes = jsonParams.optJSONArray(RequestConverter2.SELECTIONS_TERMS_EXCLUDES);
        String operator = jsonParams.optString(RequestConverter2.SELECTIONS_TERMS_OPERATOR,  RequestConverter2.SELECTIONS_TERMS_OPERATOR_OR);
        if(facet!= null && (values != null || excludes != null))
        {
          BrowseSelection sel = new BrowseSelection(facet);
          ValueOperation op = ValueOperation.ValueOperationOr;
          if(RequestConverter2.SELECTIONS_TERMS_OPERATOR_AND.equals(operator))
            op = ValueOperation.ValueOperationAnd;

          if(values != null && values.length()>0){
            sel.setValues(formatValues(facet, getStrings(values), facetInfoMap));
          }

          if(excludes != null && excludes.length()>0){
            sel.setNotValues(formatValues(facet, getStrings(excludes), facetInfoMap));
          }

          sel.setSelectionOperation(op);
          req.addSelection(sel);
        }
      }
    }
    else if(RequestConverter2.SELECTIONS_RANGE.equals(type))
    {
      Iterator<String> iter = jsonSel.keys();
      if(iter.hasNext()){
        String facet = iter.next();
        JSONObject jsonParams = jsonSel.optJSONObject(facet);

        String upper = jsonParams.optString(RequestConverter2.SELECTIONS_RANGE_TO, "*");
        String lower = jsonParams.optString(RequestConverter2.SELECTIONS_RANGE_FROM, "*");
        boolean includeUpper = jsonParams.optBoolean(RequestConverter2.SELECTIONS_RANGE_INCLUDE_UPPER, true);
        boolean includeLower = jsonParams.optBoolean(RequestConverter2.SELECTIONS_RANGE_INCLUDE_LOWER, true);
        String left = "[", right = "]";
        if(includeLower == false)
          left = "(";
        if(includeUpper == false)
          right = ")";

        String range = left + lower + " TO " + upper + right;
        if(facet!= null )
        {
          BrowseSelection sel = new BrowseSelection(facet);
          String[] vals = new String[1];
          vals[0] = range;
          sel.setValues(vals);
          req.addSelection(sel);
        }
      }
    }
    else if(RequestConverter2.SELECTIONS_PATH.equals(type))
    {
      Iterator<String> iter = jsonSel.keys();
      if(iter.hasNext()){
        String facet = iter.next();
        JSONObject jsonParams = jsonSel.optJSONObject(facet);

        String value = jsonParams.optString(RequestConverter2.SELECTIONS_PATH_VALUE, null);

        if(facet != null && value != null){
          BrowseSelection sel = new BrowseSelection(facet);
          String[] vals = new String[1];
          vals[0] = value;
          sel.setValues(vals);

          if(jsonParams.has(RequestConverter2.SELECTIONS_PATH_STRICT)){
            boolean strict = jsonParams.optBoolean(RequestConverter2.SELECTIONS_PATH_STRICT, false);
            sel.getSelectionProperties().setProperty(PathFacetHandler.SEL_PROP_NAME_STRICT, String.valueOf(strict));
          }

          if(jsonParams.has(RequestConverter2.SELECTIONS_PATH_DEPTH)){
            int depth = jsonParams.optInt(RequestConverter2.SELECTIONS_PATH_DEPTH, 1);
            sel.getSelectionProperties().setProperty(PathFacetHandler.SEL_PROP_NAME_DEPTH, String.valueOf(depth));
          }

          req.addSelection(sel);
        }
      }
    }
    else if(RequestConverter2.SELECTIONS_CUSTOM.equals(type))
    {
      ;
    }
    else if(RequestConverter2.SELECTIONS_DEFAULT.equals(type))
    {
      ;
    }
  }

  private static Map<String, String> createFacetProperties(JSONObject facetJson) {
    Map<String, String> ret = new HashMap<String, String>();
    JSONObject params = facetJson.optJSONObject("properties");
    if (params == null) {
      return ret;
    }
    Iterator<String> iter = params.keys();
    if(iter.hasNext()){
      String key = iter.next();
      Object val = params.opt(key);
      if (val != null) {
        ret.put(key, val.toString());
      }
    }
    return ret;
    
  }

  /**
   * @param jsonValues
   * @return
   * @throws JSONException
   */
  private static double[] convertJSONToDoubleArray(JSONArray jsonArray) throws JSONException
  {
    double[] doubleArray = new double[jsonArray.length()];
    if (jsonArray != null && jsonArray.length() > 0)
    {
      for (int i = 0; i < jsonArray.length(); i++)
      {
        doubleArray[i] = jsonArray.getDouble(i);
      }
    }
    return doubleArray;
  }

  /**
   * @param jsonValues
   * @return
   * @throws Exception
   */
  private static byte[] convertJSONToByteArray(JSONArray jsonArray) throws Exception
  {
    if(jsonArray != null && jsonArray.length() == 1)
    {
      String base64 = jsonArray.getString(0);
      byte[] bytes = Base64.decodeBase64(base64);
      return bytes;
    }
    else
      throw new Exception("too many base64 encoded data in one parameter");
  }

  /**
   * @param jsonValues
   * @return
   * @throws JSONException
   */
  private static long[] convertJSONToLongArray(JSONArray jsonArray) throws JSONException
  {
    long[] longArray = new long[jsonArray.length()];
    if (jsonArray != null && jsonArray.length() > 0)
    {
      for (int i = 0; i < jsonArray.length(); i++)
      {
        longArray[i] = Long.parseLong(jsonArray.getString(i));
      }
    }
    return longArray;
  }

  /**
   * @param jsonValues
   * @return
   * @throws JSONException
   */
  private static boolean[] convertJSONToBoolArray(JSONArray jsonArray) throws JSONException
  {
    boolean[] boolArray = new boolean[jsonArray.length()];
    if (jsonArray != null && jsonArray.length() > 0)
    {
      for (int i = 0; i < jsonArray.length(); i++)
      {
        boolArray[i] = jsonArray.getBoolean(i);
      }
    }
    return boolArray;
  }

  /**
   * @param jsonValues
   * @return
   * @throws JSONException
   */
  private static List<String> convertJSONToStringArray(JSONArray jsonArray) throws JSONException
  {
    List<String> arString = new ArrayList<String>();
    if (jsonArray != null && jsonArray.length() > 0)
    {
      for (int i = 0; i < jsonArray.length(); i++)
      {
        arString.add(jsonArray.getString(i));
      }
    }
    return arString;
  }

  /**
   * @param jsonValues
   * @return
   * @throws JSONException
   */
  private static int[] convertJSONToIntArray(JSONArray jsonArray) throws JSONException
  {
    int[] intArray = new int[jsonArray.length()];
    if (jsonArray != null && jsonArray.length() > 0)
    {
      for (int i = 0; i < jsonArray.length(); i++)
      {
        intArray[i] = jsonArray.getInt(i);
      }
    }
    return intArray;
  }
}
