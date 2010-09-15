package com.sensei.search.util;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;

import com.browseengine.bobo.api.BrowseRequest;
import com.browseengine.bobo.api.BrowseSelection;
import com.sensei.search.nodes.SenseiQueryBuilder;
import com.sensei.search.nodes.SenseiQueryBuilderFactory;
import com.sensei.search.req.SenseiRequest;

public class RequestConverter {
	public static BrowseRequest convert(SenseiRequest req, SenseiQueryBuilderFactory queryBuilderFactory) throws Exception{
		BrowseRequest breq = new BrowseRequest();
		breq.setTid(req.getTid());
		breq.setOffset(req.getOffset());
		breq.setCount(req.getCount());
		breq.setSort(req.getSort());
		breq.setFetchStoredFields(req.isFetchStoredFields());
		breq.setShowExplanation(req.isShowExplanation());
		
		SenseiQueryBuilder queryBuilder = queryBuilderFactory.getQueryBuilder(req.getQuery());
       
        // query
        Query q = null;
        
        if (queryBuilder!=null){
        	q = queryBuilder.buildQuery();
        }
        
        if(q != null){
            breq.setQuery(q);
        }
        
        // filter
        Filter f = queryBuilder.buildFilter();
        if(f != null){
            breq.setFilter(f);
        }
        
		// selections
		BrowseSelection[] sels = req.getSelections();
		for (BrowseSelection sel : sels){
			breq.addSelection(sel);
		}
		// transfer RuntimeFacetHandler init parameters
		breq.setFacetHandlerDataMap(req.getAllFacetHandlerInitializerParams());
		// facetspecs
		breq.setFacetSpecs(req.getFacetSpecs());
		// filter ids
		long[] filterIds = req.getFilterUids();
		if (filterIds!=null){
			boolean isFilterOut = req.isFilterOutIds();
			//breq.setFilter(new UID)
		}
		return breq;
	}
}
