package com.sensei.search.req;

import com.browseengine.bobo.api.BrowseHit;
import com.browseengine.bobo.api.BrowseResult;

public class SenseiResult extends BrowseResult{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String _parsedQuery = null;
	
	public SenseiHit[] getSenseiHits()
	{
	  BrowseHit[] hits = getHits();
	  if (hits==null || hits.length == 0){
		  return new SenseiHit[0];
	  }
	  return (SenseiHit[])hits;
	}
	
	public void setParsedQuery(String query){
		_parsedQuery = query;
	}
	
	public String getParsedQuery(){
		return _parsedQuery;
	}
}
