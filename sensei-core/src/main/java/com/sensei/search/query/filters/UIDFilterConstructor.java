package com.sensei.search.query.filters;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.facets.FacetHandler;
import com.sensei.conf.SenseiFacetHandlerBuilder;
import com.sensei.search.facet.UIDFacetHandler;
import com.sensei.search.query.FilterConstructor;
import com.sensei.search.util.RequestConverter2;

public class UIDFilterConstructor  extends FilterConstructor{

	@Override
	public Filter constructFilter(final JSONObject json) throws Exception {
		return new Filter(){

		    @Override
			public DocIdSet getDocIdSet(IndexReader reader)
					throws IOException {
				if (reader instanceof BoboIndexReader){
					BoboIndexReader boboReader = (BoboIndexReader)reader;
					FacetHandler uidHandler = boboReader.getFacetHandler(SenseiFacetHandlerBuilder.UID_FACET_NAME);
					if (uidHandler!=null && uidHandler instanceof UIDFacetHandler){
						UIDFacetHandler uidFacet = (UIDFacetHandler)uidHandler;
						try{
						  String[] vals = RequestConverter2.getStrings(json.optJSONArray(VALUES_PARAM));
						  String[] nots = RequestConverter2.getStrings(json.optJSONArray(EXCLUDES_PARAM));
						  BrowseSelection uidSel = new BrowseSelection(SenseiFacetHandlerBuilder.UID_FACET_NAME);
						  uidSel.setValues(vals);
						  uidSel.setNotValues(nots);
						  return uidFacet.buildFilter(uidSel).getDocIdSet(boboReader);
						}
						catch(Exception e){
							throw new IOException(e.getMessage());
						}
					}
					else{
						throw new IllegalStateException("invalid uid handler "+uidHandler);
					}
				}
				else{
					throw new IllegalStateException("read not instance of "+BoboIndexReader.class);
				}
			}
		
		};
	}
	
}
