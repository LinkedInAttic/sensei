package com.sensei.search.util;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;

import com.browseengine.bobo.api.BrowseRequest;
import com.browseengine.bobo.api.BrowseSelection;
import com.sensei.search.req.SenseiRequest;

public class RequestConverter {
	public static BrowseRequest convert(SenseiRequest req,QueryParser qparser) throws ParseException{
		BrowseRequest breq = new BrowseRequest();
		breq.setOffset(req.getOffset());
		breq.setCount(req.getCount());
		breq.setSort(req.getSort());
		breq.setFetchStoredFields(req.isFetchStoredFields());
		
		// query
		String qString = req.getQuery();
		if (qString != null && qString.length()>0){
			Query q = qparser.parse(qString);
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
