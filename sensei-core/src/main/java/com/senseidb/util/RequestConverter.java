package com.senseidb.util;


import com.linkedin.bobo.api.BrowseRequest;
import com.linkedin.bobo.api.BrowseSelection;
import com.senseidb.search.node.SenseiQueryBuilder;
import com.senseidb.search.node.SenseiQueryBuilderFactory;
import com.senseidb.search.req.SenseiRequest;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;

public class RequestConverter {
	private static Logger logger = Logger.getLogger(RequestConverter.class);
	public static BrowseRequest convert(SenseiRequest req, SenseiQueryBuilderFactory queryBuilderFactory) throws Exception{
		BrowseRequest breq = new BrowseRequest();
		breq.setTid(req.getTid());
		breq.setOffset(req.getOffset());
		breq.setCount(req.getCount());
		breq.setSort(req.getSort());
		breq.setFetchStoredFields(req.isFetchStoredFields());
		breq.setShowExplanation(req.isShowExplanation());
		breq.setTermVectorsToFetch(req.getTermVectorsToFetch());
    breq.setGroupBy(req.getGroupBy());
    breq.setMaxPerGroup(req.getMaxPerGroup());
    if (breq.getGroupBy() != null && breq.getMaxPerGroup() > 1) {
      breq.setCollectDocIdCache(true);
    }
		
		SenseiQueryBuilder queryBuilder = queryBuilderFactory.getQueryBuilder(req.getQuery());
       
        // query
        Query q = null;
        Filter f = null;
        
        if (queryBuilder!=null){
        	q = queryBuilder.buildQuery();
        	f = queryBuilder.buildFilter();
        }
        
        if(q != null){
            breq.setQuery(q);
        }
        
        if(f != null){
            breq.setFilter(f);
        }
        
		// selections
		BrowseSelection[] sels = req.getSelections();
		for (BrowseSelection sel : sels){
			breq.addSelection(sel);
		}
		// transfer RuntimeFacetHandler init parameters
		breq.setFacetHandlerDataMap(req.getFacetHandlerInitParamMap());
		// facetspecs
		breq.setFacetSpecs(req.getFacetSpecs());
		// filter ids
		// TODO: needs to some how hook this up
		return breq;
	}
	
	public static Map<String,Configuration> parseParamConf(Configuration params,String prefix){
		Iterator<String> keys = params.getKeys(prefix);
		HashMap<String,Configuration> map = new HashMap<String,Configuration>();
		
		while(keys.hasNext()){
			try{
			  String key = keys.next();
			  String[] values = params.getStringArray(key);
			  
			  String subString = key.substring(prefix.length()+1);
			  
			  String[] parts = subString.split("\\.");
			  if (parts.length == 2){
				  String name = parts[0];
				  String paramName = parts[1];
				  Configuration conf = map.get(name);
				  
				  if (conf == null){
					  conf = new BaseConfiguration();
					  map.put(name, conf);
				  }
				  
				  for (String val : values){
				    conf.addProperty(paramName,val);
				  }
			  } else if (parts.length == 3) {
          // parse facet sub-parameters e.g. for dynamic facet init params
          String facetName = parts[0];
          String paramName = parts[1];
          String paramAttrName = parts[2];

          Configuration conf = map.get(facetName);
          if (conf == null){
            conf = new BaseConfiguration();
            map.put(facetName, conf);
          }

          Configuration paramConf;
          if (conf.getProperty(paramName) == null) {
            paramConf = new BaseConfiguration();
            conf.addProperty(paramName, paramConf);
          } else {
            paramConf = (Configuration)conf.getProperty(paramName);
          }

          for (String val : values){
            paramConf.addProperty(paramAttrName,val);
          }
        }
			  else{
				  logger.error("invalid param format: "+key);
			  }
			}
			catch(Exception e){
			  logger.error(e.getMessage(),e);
			}
		}
		return map;
	}
	
	
}
