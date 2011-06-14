package com.sensei.conf;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.RuntimeFacetHandlerFactory;
import com.browseengine.bobo.facets.data.PredefinedTermListFactory;
import com.browseengine.bobo.facets.data.TermListFactory;
import com.browseengine.bobo.facets.impl.CompactMultiValueFacetHandler;
import com.browseengine.bobo.facets.impl.MultiValueFacetHandler;
import com.browseengine.bobo.facets.impl.PathFacetHandler;
import com.browseengine.bobo.facets.impl.RangeFacetHandler;
import com.browseengine.bobo.facets.impl.SimpleFacetHandler;
import com.sensei.indexing.api.DefaultSenseiInterpreter;
import com.sensei.search.req.SenseiSystemInfo;

public class SenseiFacetHandlerBuilder {

	private static Logger logger = Logger
			.getLogger(SenseiFacetHandlerBuilder.class);

	private static Map<String, TermListFactory<?>> getPredefinedTermListFactoryMap(Document schemaDoc) throws ConfigurationException {
		HashMap<String, TermListFactory<?>> retMap = new HashMap<String, TermListFactory<?>>();
		NodeList columns = schemaDoc.getElementsByTagName("column");
		for (int i = 0; i < columns.getLength(); ++i) {
			try {
				Element column = (Element) columns.item(i);
				String n = column.getAttribute("name");
				String t = column.getAttribute("type");

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
						f = column.getAttribute("format");
					} catch (Exception ex) {
						logger.error(ex.getMessage(), ex);
					}

					if (f.isEmpty())
						throw new Exception("Date format cannot be empty.");
					else
						factory = new PredefinedTermListFactory<Date>(
								Date.class, f);
				}
				
				if (factory!=null){
				  retMap.put(n, factory);
				}

			} catch (Exception e) {
				throw new ConfigurationException("Error parsing schema: "
						+ columns.item(i), e);
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
	
	static PathFacetHandler buildPathHandler(String name,String fieldName,Set<String> depends,Map<String,List<String>> paramMap){
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
		LinkedList<String> predefinedRanges = new LinkedList<String>();
		if (paramMap!=null){
			List<String> rangeList = paramMap.get("range");
			if (rangeList!=null){
			  for (String range : rangeList){
				if (!range.matches("\\[.* TO .*\\]")){
					range = "["
							+ range.replaceFirst("[-,]", " TO ")
							+ "]";	
				}
				predefinedRanges.add(range);
			  }
			}
		}
		return new RangeFacetHandler(name,fieldName,termListFactory,predefinedRanges);
	}
	
	static Map<String,List<String>> parseParams(NodeList paramList){
		HashMap<String,List<String>> retmap = new HashMap<String,List<String>>();
		if (paramList!=null){
		  for (int j = 0; j < paramList.getLength(); ++j) {
		
			Element param = (Element) paramList.item(j);
			String paramName = param.getAttribute("name");
			String paramValue = param.getAttribute("value");
			
			List<String>list = retmap.get(paramName);
			if (list==null){
				list = new LinkedList<String>();
				retmap.put(paramName, list);
			}

			list.add(paramValue);
			
			/*if (paramName.equals("range")) {
				if (!paramValue.matches("\\[.* TO .*\\]"))
					paramValue = "["
							+ paramValue.replaceFirst("[-,]", " TO ")
							+ "]";
				rangeList.add(paramValue);
			} else {
				// Set the bean properties.
				Class pType = PropertyUtils.getPropertyType(
						facetHandler, paramName);
				if (pType == null) // No such properties.
					continue;
				Object objValue = paramValue;
				try {
					Constructor ctor = pType
							.getConstructor(String.class);
					objValue = ctor.newInstance(paramValue);
				} catch (NoSuchMethodException ex) {
				}
				PropertyUtils.setProperty(facetHandler, paramName,
						objValue);
			}
			*/
		  }
		}
		
		return retmap;
	}

	public static SenseiSystemInfo buildFacets(Document schemaDoc,
			ApplicationContext customFacetContext,List<FacetHandler<?>> facets,List<RuntimeFacetHandlerFactory<?,?>> runtimeFacets)
			throws ConfigurationException {

    SenseiSystemInfo sysInfo = new SenseiSystemInfo();
		NodeList facetsList = schemaDoc.getElementsByTagName("facets");
		Element facetsEle = null;
		if (facetsList.getLength() > 0) {
			facetsEle = (Element) facetsList.item(0);
		} else {
			return sysInfo;
		}
		
		NodeList facetList = facetsEle.getElementsByTagName("facet");

		if (facetList.getLength() <= 0)
			return sysInfo;

		Map<String, TermListFactory<?>> termListFactoryMap = getPredefinedTermListFactoryMap(schemaDoc);

    Set<SenseiSystemInfo.SenseiFacetInfo> facetInfos = new HashSet<SenseiSystemInfo.SenseiFacetInfo>();
		for (int i = 0; i < facetList.getLength(); ++i) {
			try {
				Element facet = (Element) facetList.item(i);
				String name = facet.getAttribute("name");
				String type = facet.getAttribute("type");
				String fieldName = facet.getAttribute("column");
				if (fieldName==null || fieldName.isEmpty()){
					fieldName = name;
				}
				Set<String> dependSet = new HashSet<String>();
				String depends= facet.getAttribute("depends");
				if (depends != null) {
					for (String dep :depends.split("[,; ]")) {
						dep = dep.trim();
						if (!dep.equals("")) {
							dependSet.add(dep);
						}
					}
				}

        SenseiSystemInfo.SenseiFacetInfo facetInfo = new SenseiSystemInfo.SenseiFacetInfo(name);
        Map<String, String> facetProps = new HashMap<String, String>();
        facetProps.put("type", type);
        facetProps.put("column", fieldName);
        facetProps.put("depends", dependSet.toString());

				NodeList paramList = facet.getElementsByTagName("param");
				Map<String,List<String>> paramMap = parseParams(paramList);
        for (String key : paramMap.keySet()) {
          facetProps.put(key, paramMap.get(key).toString());
        }

        facetInfo.setProps(facetProps);
        facetInfos.add(facetInfo);

				FacetHandler<?> facetHandler = null;
				if (type.equals("simple")) {
					facetHandler = buildSimpleFacetHandler(name, fieldName, dependSet, termListFactoryMap.get(fieldName));
				} else if (type.equals("path")) {
					facetHandler = buildPathHandler(name, fieldName, dependSet, paramMap);
				} else if (type.equals("range")) {
					facetHandler = buildRangeHandler(name, fieldName, termListFactoryMap.get(fieldName), paramMap);
				} else if (type.equals("multi")) {
					facetHandler = buildMultiHandler(name, fieldName,  termListFactoryMap.get(fieldName), dependSet);
				} else if (type.equals("compact-multi")) {
					facetHandler = buildCompactMultiHandler(name, fieldName, dependSet,  termListFactoryMap.get(fieldName));
				} else if (type.equals("custom")) {
					
					boolean isDynamic = Boolean.parseBoolean(facet.getAttribute("dynamic"));
					// Load from custom-facets spring configuration.
					if (isDynamic){
					   RuntimeFacetHandlerFactory<?,?> runtimeFacetFactory = (RuntimeFacetHandlerFactory<?,?>)customFacetContext.getBean(name);
					   runtimeFacets.add(runtimeFacetFactory);
					}
					else{
						facetHandler = (FacetHandler<?>) customFacetContext.getBean(name);
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
						+ facetList.item(i), e);
			}
		}
    sysInfo.setFacetInfos(facetInfos);

    return sysInfo;
	}
}
