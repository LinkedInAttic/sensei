package com.sensei.search.util;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Query;

import com.browseengine.bobo.api.BrowseRequest;
import com.browseengine.bobo.api.BrowseSelection;
import com.sensei.search.nodes.SenseiQueryBuilder;
import com.sensei.search.req.SenseiRequest;

public class RequestConverter {
	public static BrowseRequest convert(SenseiRequest req, SenseiQueryBuilder queryBuilder) throws ParseException{
		BrowseRequest breq = new BrowseRequest();
		breq.setOffset(req.getOffset());
		breq.setCount(req.getCount());
		breq.setSort(req.getSort());
		breq.setFetchStoredFields(req.isFetchStoredFields());
		
		// query
		Query q = queryBuilder.buildQuery(req);
		if(q != null){
			breq.setQuery(q);
		}
		
		// selections
		BrowseSelection[] sels = req.getSelections();
		for (BrowseSelection sel : sels){
			req.addSelection(sel);
		}
		// facetspecs
		breq.setFacetSpecs(req.getFacetSpecs());
		// filter ids
		// TODO: needs to some how hook this up
		return breq;
	}
}
