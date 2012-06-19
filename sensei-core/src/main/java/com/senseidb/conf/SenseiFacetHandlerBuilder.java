package com.senseidb.conf;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.util.Assert;

import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.FacetHandler.FacetDataNone;
import com.browseengine.bobo.facets.FacetHandlerInitializerParam;
import com.browseengine.bobo.facets.RuntimeFacetHandler;
import com.browseengine.bobo.facets.RuntimeFacetHandlerFactory;
import com.browseengine.bobo.facets.AbstractRuntimeFacetHandlerFactory;
import com.browseengine.bobo.facets.attribute.AttributesFacetHandler;
import com.browseengine.bobo.facets.data.PredefinedTermListFactory;
import com.browseengine.bobo.facets.data.TermListFactory;
import com.browseengine.bobo.facets.impl.CompactMultiValueFacetHandler;
import com.browseengine.bobo.facets.impl.DynamicTimeRangeFacetHandler;
import com.browseengine.bobo.facets.impl.HistogramFacetHandler;
import com.browseengine.bobo.facets.impl.MultiValueFacetHandler;
import com.browseengine.bobo.facets.impl.MultiValueWithWeightFacetHandler;
import com.browseengine.bobo.facets.impl.PathFacetHandler;
import com.browseengine.bobo.facets.impl.RangeFacetHandler;
import com.browseengine.bobo.facets.impl.SimpleFacetHandler;
import com.browseengine.bobo.facets.range.MultiRangeFacetHandler;
import com.senseidb.conf.SenseiSchema.FacetDefinition;
import com.senseidb.indexing.DefaultSenseiInterpreter;
import com.senseidb.indexing.activity.ActivityIntValues;
import com.senseidb.indexing.activity.ActivityValues;
import com.senseidb.indexing.activity.CompositeActivityManager;
import com.senseidb.indexing.activity.CompositeActivityValues;
import com.senseidb.indexing.activity.facet.ActivityRangeFacetHandler;
import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.facet.UIDFacetHandler;
import com.senseidb.search.plugin.PluggableSearchEngineManager;
import com.senseidb.search.req.SenseiSystemInfo;

public class SenseiFacetHandlerBuilder {

	private static Logger logger = Logger
			.getLogger(SenseiFacetHandlerBuilder.class);

	public static String UID_FACET_NAME = "_uid";

	private static Map<String, TermListFactory<?>> getPredefinedTermListFactoryMap(JSONObject schemaObj) throws JSONException,ConfigurationException {
		HashMap<String, TermListFactory<?>> retMap = new HashMap<String, TermListFactory<?>>();
		JSONObject tableElem = schemaObj.optJSONObject("table");
    if (tableElem==null){
      throw new ConfigurationException("empty schema");
    }
		JSONArray columns = tableElem.optJSONArray("columns");

    int count = 0;
    if (columns!=null){
      count = columns.length();
    }

    for (int i = 0; i < count; ++i) {
      JSONObject column = columns.getJSONObject(i);
			try {
				String n = column.getString("name");
				String t = column.getString("type");

				TermListFactory<?> factory = null;

				if (t.equals("int")) {
					factory = DefaultSenseiInterpreter
            .getTermListFactory(int.class);
				} else if (t.equals("short")) {
					factory = DefaultSenseiInterpreter
            .getTermListFactory(short.class);
				} else if (t.equals("long")) {
					factory = DefaultSenseiInterpreter
            .getTermListFactory(long.class);
				} else if (t.equals("float")) {
					factory = DefaultSenseiInterpreter
            .getTermListFactory(float.class);
				} else if (t.equals("double")) {
					factory = DefaultSenseiInterpreter
            .getTermListFactory(double.class);
				} else if (t.equals("char")) {
					factory = DefaultSenseiInterpreter
            .getTermListFactory(char.class);
				} else if (t.equals("string")) {
					factory = TermListFactory.StringListFactory;
				} else if (t.equals("boolean")) {
					factory = DefaultSenseiInterpreter
            .getTermListFactory(boolean.class);
				} else if (t.equals("date")) {

					String f = "";
					try {
						f = column.optString("format");
					} catch (Exception ex) {
						logger.error(ex.getMessage(), ex);
					}

					if (f.isEmpty())
						throw new Exception("Date format cannot be empty.");
					else
						factory = new PredefinedTermListFactory<Date>(Date.class, f);
				}

				if (factory!=null){
				  retMap.put(n, factory);
				}

			} catch (Exception e) {
				throw new ConfigurationException("Error parsing schema: "
                                         + column, e);
			}
		}
		return retMap;
	}

	static SimpleFacetHandler buildSimpleFacetHandler(String name,String fieldName,Set<String> depends,TermListFactory<?> termListFactory){
		return new SimpleFacetHandler(name, fieldName, termListFactory, depends);
	}

	static CompactMultiValueFacetHandler buildCompactMultiHandler(String name,String fieldName,Set<String> depends,TermListFactory<?> termListFactory){
		// compact multi should honor depends
		return new CompactMultiValueFacetHandler(name, fieldName, termListFactory);
	}

	static MultiValueFacetHandler buildMultiHandler(String name,String fieldName,TermListFactory<?> termListFactory,Set<String> depends){
		return new MultiValueFacetHandler(name, fieldName, termListFactory,null,depends);
	}
	
	static MultiValueFacetHandler buildWeightedMultiHandler(String name,String fieldName,TermListFactory<?> termListFactory,Set<String> depends){
	    return new MultiValueWithWeightFacetHandler(name, fieldName, termListFactory);
	}

	static PathFacetHandler buildPathHandler(String name,String fieldName, Map<String,List<String>> paramMap){
		PathFacetHandler handler = new PathFacetHandler(name, fieldName, false);	// path does not support multi value yet
		String sep = null;
		if (paramMap!=null){
			List<String> sepVals = paramMap.get("separator");
			if (sepVals!=null && sepVals.size()>0){
				sep = sepVals.get(0);
			}
		}
		if (sep!=null){
		  handler.setSeparator(sep);
		}
		return handler;
	}

	static RangeFacetHandler buildRangeHandler(String name,String fieldName,TermListFactory<?> termListFactory,Map<String,List<String>> paramMap){
		LinkedList<String> predefinedRanges = buildPredefinedRanges(paramMap);
		return new RangeFacetHandler(name,fieldName,termListFactory,predefinedRanges);
	}

  private static LinkedList<String> buildPredefinedRanges(Map<String, List<String>> paramMap) {
    LinkedList<String> predefinedRanges = new LinkedList<String>();
		if (paramMap!=null){
			List<String> rangeList = paramMap.get("range");
			if (rangeList!=null){
			  for (String range : rangeList){
				if (!range.matches("\\[.* TO .*\\]")){
					if (!range.contains("-") || !range.contains(",")) {
				  range = "[" + range.replaceFirst("[-,]", " TO ") + "]";
					} else {
					  range = "[" + range.replaceFirst(",", " TO ") + "]";
					}
				}
				predefinedRanges.add(range);
			  }
			}
		}
    return predefinedRanges;
  }

	public static Map<String,List<String>> parseParams(JSONArray paramList) throws JSONException{
		HashMap<String,List<String>> retmap = new HashMap<String,List<String>>();
		if (paramList!=null){
		  int count = paramList.length();
		  for (int j = 0; j < count; ++j) {
		    JSONObject param = paramList.getJSONObject(j);
			String paramName = param.getString("name");
			String paramValue = param.getString("value");

			List<String>list = retmap.get(paramName);
			if (list==null){
				list = new LinkedList<String>();
				retmap.put(paramName, list);
			}

			list.add(paramValue);
		  }
		}

		return retmap;
	}

  private static String getRequiredSingleParam(Map<String, List<String>> paramMap,
                                               String name)
    throws ConfigurationException
  {
    if (paramMap != null)
    {
      List<String> vals = paramMap.get(name);
      if (vals != null && vals.size() > 0)
      {
        return vals.get(0);
      }
      else
      {
        throw new ConfigurationException("Parameter " + name + " is missing.");
      }
    }
    else
    {
      throw new ConfigurationException("Parameter map is null.");
    }
  }

  private static RuntimeFacetHandlerFactory<?, ?> getHistogramFacetHandlerFactory(JSONObject facet,
                                                                                  String name,
                                                                                  Map<String,List<String>> paramMap)
    throws ConfigurationException
  {
    String dataType = getRequiredSingleParam(paramMap, "datatype");
    String dataHandler = getRequiredSingleParam(paramMap, "datahandler");
    String startParam = getRequiredSingleParam(paramMap, "start");
    String endParam = getRequiredSingleParam(paramMap, "end");
    String unitParam = getRequiredSingleParam(paramMap, "unit");

    if ("int".equals(dataType))
    {
      int start = Integer.parseInt(startParam);
      int end = Integer.parseInt(endParam);
      int unit = Integer.parseInt(unitParam);
      return buildHistogramFacetHandlerFactory(name, dataHandler, start, end, unit);
    }
    else if ("short".equals(dataType))
    {
      short start = (short) Integer.parseInt(startParam);
      short end = (short) Integer.parseInt(endParam);
      short unit = (short) Integer.parseInt(unitParam);
      return buildHistogramFacetHandlerFactory(name, dataHandler, start, end, unit);
    }
    else if ("long".equals(dataType))
    {
      long start = Long.parseLong(startParam);
      long end = Long.parseLong(endParam);
      long unit = Long.parseLong(unitParam);
      return buildHistogramFacetHandlerFactory(name, dataHandler, start, end, unit);
    }
    else if ("float".equals(dataType))
    {
      float start = Float.parseFloat(startParam);
      float end = Float.parseFloat(endParam);
      float unit = Float.parseFloat(unitParam);
      return buildHistogramFacetHandlerFactory(name, dataHandler, start, end, unit);
    }
    else if ("double".equals(dataType))
    {
      double start = Double.parseDouble(startParam);
      double end = Double.parseDouble(endParam);
      double unit = Double.parseDouble(unitParam);
      return buildHistogramFacetHandlerFactory(name, dataHandler, start, end, unit);
    }
    return null;
  }

	private static <T extends Number> RuntimeFacetHandlerFactory<?,?> buildHistogramFacetHandlerFactory(final String name,
	                                                                                                    final String dataHandler,
	                                                                                                    final T start,
	                                                                                                    final T end,
	                                                                                                    final T unit)
	{
	  return new AbstractRuntimeFacetHandlerFactory<FacetHandlerInitializerParam, RuntimeFacetHandler<FacetDataNone>>()
	  {
	    @Override
	    public RuntimeFacetHandler<FacetDataNone> get(FacetHandlerInitializerParam params)
	    {
	      return new HistogramFacetHandler<T>(name, dataHandler, start, end, unit);
	    };

      @Override
      public boolean isLoadLazily()
      {
        return true;
      }

	    @Override
	    public String getName()
	    {
	      return name;
	    }
	  };
	}

	public static SenseiSystemInfo buildFacets(JSONObject schemaObj, SenseiPluginRegistry pluginRegistry,
			List<FacetHandler<?>> facets,List<RuntimeFacetHandlerFactory<?,?>> runtimeFacets, PluggableSearchEngineManager pluggableSearchEngineManager)
    throws JSONException,ConfigurationException {
	  Set<String> pluggableSearchEngineFacetNames = pluggableSearchEngineManager.getFacetNames();
    SenseiSystemInfo sysInfo = new SenseiSystemInfo();
    JSONArray facetsList = schemaObj.optJSONArray("facets");

		int count = 0;

		if (facetsList!=null){
		  count = facetsList.length();
		}

		if (count <= 0) {
			return sysInfo;
		}

    JSONObject table = schemaObj.optJSONObject("table");
    if (table == null)
    {
      throw new ConfigurationException("Empty schema");
    }
    JSONArray columns = table.optJSONArray("columns");
    Map<String, JSONObject> columnMap = new HashMap<String, JSONObject>();
    for (int i = 0; i < columns.length(); ++i)
    {
      JSONObject column = columns.getJSONObject(i);
      try
      {
        String name = column.getString("name");
        columnMap.put(name, column);
      }
      catch (Exception e) {
        throw new ConfigurationException("Error parsing schema: ", e);
      }
    }

		Map<String, TermListFactory<?>> termListFactoryMap = getPredefinedTermListFactoryMap(schemaObj);

        Set<SenseiSystemInfo.SenseiFacetInfo> facetInfos = new HashSet<SenseiSystemInfo.SenseiFacetInfo>();
		for (int i = 0; i < count; ++i) {

      JSONObject facet = facetsList.getJSONObject(i);
			try {
				String name = facet.getString("name");
				if (UID_FACET_NAME.equals(name)){
					logger.error("facet name: "+UID_FACET_NAME+" is reserved, skipping...");
					continue;
				}
				if (pluggableSearchEngineFacetNames.contains(name)) {
				  continue;
				}
				String type = facet.getString("type");
				String fieldName = facet.optString("column",name);
				Set<String> dependSet = new HashSet<String>();
				JSONArray dependsArray = facet.optJSONArray("depends");
				
				if (dependsArray != null) {
				  int depCount = dependsArray.length();
				  for (int k=0;k<depCount;++k){
				    dependSet.add(dependsArray.getString(k));
				  }
				}

        SenseiSystemInfo.SenseiFacetInfo facetInfo = new SenseiSystemInfo.SenseiFacetInfo(name);
        Map<String, String> facetProps = new HashMap<String, String>();
        facetProps.put("type", type);
        facetProps.put("column", fieldName);
        JSONObject column = columnMap.get(fieldName);
        String columnType = (column == null) ? "" : column.optString("type", "");
        facetProps.put("column_type", columnType);
        facetProps.put("depends", dependSet.toString());

				JSONArray paramList = facet.optJSONArray("params");

				Map<String,List<String>> paramMap = parseParams(paramList);

        for (Entry<String,List<String>> entry : paramMap.entrySet()) {
          facetProps.put(entry.getKey(), entry.getValue().toString());
        }

        facetInfo.setProps(facetProps);
        facetInfos.add(facetInfo);

				FacetHandler<?> facetHandler = null;
				if (type.equals("simple")) {
					facetHandler = buildSimpleFacetHandler(name, fieldName, dependSet, termListFactoryMap.get(fieldName));
				} else if (type.equals("path")) {
					facetHandler = buildPathHandler(name, fieldName, paramMap);
				} else if (type.equals("range")) {
					if (column.optBoolean("multi")) {
					  facetHandler = new MultiRangeFacetHandler(name, fieldName, null,  termListFactoryMap.get(fieldName) , buildPredefinedRanges(paramMap));
					} else {
					  facetHandler = buildRangeHandler(name, fieldName, termListFactoryMap.get(fieldName), paramMap);
					}
				}  else if (type.equals("multi")) {
					facetHandler = buildMultiHandler(name, fieldName,  termListFactoryMap.get(fieldName), dependSet);
				} else if (type.equals("compact-multi")) {
					facetHandler = buildCompactMultiHandler(name, fieldName, dependSet,  termListFactoryMap.get(fieldName));

				} else if (type.equals("weighted-multi")) {
				    facetHandler = buildWeightedMultiHandler(name, fieldName,  termListFactoryMap.get(fieldName), dependSet);
        } else if (type.equals("attribute")) {
          facetHandler = new AttributesFacetHandler(name, fieldName, termListFactoryMap.get(fieldName), null, facetProps);
        } else if (type.equals("histogram")) {
				  // A histogram facet handler is always dynamic
				  RuntimeFacetHandlerFactory<?, ?> runtimeFacetFactory = getHistogramFacetHandlerFactory(facet, name, paramMap);
				  runtimeFacets.add(runtimeFacetFactory);
				  facetInfo.setRunTime(true);
				}  else if (type.equals("dynamicTimeRange")) {
				  if (dependSet.isEmpty()) {
			      Assert.isTrue(fieldName != null && fieldName.length() > 0, "Facet handler " + name + " requires either depends or column attributes");
			      RangeFacetHandler internalFacet = new RangeFacetHandler(name + "_internal", fieldName, new PredefinedTermListFactory(Long.class, DynamicTimeRangeFacetHandler.NUMBER_FORMAT), null);
			      facets.add(internalFacet);
			      dependSet.add(internalFacet.getName());
				  }
          RuntimeFacetHandlerFactory<?, ?> runtimeFacetFactory = getDynamicTimeFacetHandlerFactory(name, fieldName, dependSet, paramMap);
          runtimeFacets.add(runtimeFacetFactory);
          facetInfo.setRunTime(true);

        } else if (type.equals("custom")) {
					boolean isDynamic = facet.optBoolean("dynamic");
					// Load from custom-facets spring configuration.
					if (isDynamic){
            RuntimeFacetHandlerFactory<?,?> runtimeFacetFactory = pluginRegistry.getRuntimeFacet(name);
            runtimeFacets.add(runtimeFacetFactory);
            facetInfo.setRunTime(true);
					}
					else{
						facetHandler = pluginRegistry.getFacet(name);
					}
				}
				else{
					throw new IllegalArgumentException("type not supported: "+type);
				}

				if (facetHandler!=null){
				  facets.add(facetHandler);
				}
			} catch (Exception e) {
				throw new ConfigurationException("Error parsing schema: "
                                         + facet, e);
			}
		}

		facets.addAll((Collection<? extends FacetHandler<?>>) pluggableSearchEngineManager.createFacetHandlers());
		// uid facet handler to be added by default
		UIDFacetHandler uidHandler = new UIDFacetHandler(UID_FACET_NAME);
		facets.add(uidHandler);
    sysInfo.setFacetInfos(facetInfos);

    return sysInfo;
	}

  

  public static RuntimeFacetHandlerFactory<?, ?> getDynamicTimeFacetHandlerFactory(final String name, String fieldName, Set<String> dependSet,
      final Map<String, List<String>> paramMap) {

    Assert.isTrue(dependSet.size() == 1, "Facet handler " + name + " should rely only on exactly one another facet handler, but accodring to config the depends set is " + dependSet);
    final String depends = dependSet.iterator().next();
    Assert.notEmpty(paramMap.get("range"), "Facet handler " + name + " should have at least one predefined range");

    return new AbstractRuntimeFacetHandlerFactory<FacetHandlerInitializerParam, RuntimeFacetHandler<?>>() {

      @Override
      public String getName() {
        return name;
      }

      @Override
      public  RuntimeFacetHandler<?> get(FacetHandlerInitializerParam params) {
        long overrideNow = -1;
        try {
          String overrideProp = System.getProperty("override.now");
          if (overrideProp != null) {
            overrideNow = Long.parseLong(overrideProp);
          }
        }
        catch(Exception e) {
          logger.error(e.getMessage(), e);
        }

        long now = System.currentTimeMillis();
        if (overrideNow > 0)
          now = overrideNow;
        else {
          if (params != null) {
            long[] longParam = params.getLongParam("now");
            if (longParam == null || longParam.length == 0)
              longParam = params.getLongParam("time");  // time will also work

            if (longParam != null && longParam.length > 0)
              now = longParam[0];
          }
        }

        List<String> ranges = paramMap.get("range");

        try {
          return new DynamicTimeRangeFacetHandler(name, depends, now, ranges);
        } catch (ParseException ex) {
          throw new RuntimeException(ex);
        }
      }
    };
  }
}
