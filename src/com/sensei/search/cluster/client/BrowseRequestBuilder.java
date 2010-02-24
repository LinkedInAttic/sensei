package com.sensei.search.cluster.client;

import org.apache.lucene.search.SortField;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.sensei.search.req.SenseiRequest;

public class BrowseRequestBuilder {
	private SenseiRequest _req;
	public BrowseRequestBuilder(){
		clear();
	}
	
	public void addSelection(String name,String val,boolean isNot){
		BrowseSelection sel = _req.getSelection(name);
		if (sel==null){
			sel = new BrowseSelection(name);
		}
		if (isNot){
			sel.addNotValue(val);
		}
		else{
			sel.addValue(val);
		}
		_req.addSelection(sel);
	}
	
	public void clearSelection(String name){
		_req.removeSelection(name);
	}
	
	public void applyFacetSpec(String name,int minHitCount,int maxCount,boolean expand,FacetSortSpec orderBy){
		FacetSpec fspec = new FacetSpec();
		fspec.setMinHitCount(minHitCount);
		fspec.setMaxCount(maxCount);
		fspec.setExpandSelection(expand);
		fspec.setOrderBy(orderBy);
		_req.setFacetSpec(name, fspec);
	}
	
	public void applySort(SortField[] sorts){
		if (sorts==null){
			_req.clearSort();
		}
		else{
			_req.setSort(sorts);
		}
	}
	
	public void clearFacetSpecs(){
		_req.getFacetSpecs().clear();
	}
	public void clearFacetSpec(String name){
		_req.getFacetSpecs().remove(name);
	}
	
	public void setOffset(int offset){
		_req.setOffset(offset);
	}
	
	public void setCount(int count){
		_req.setCount(count);
	}
	
	public void setQuery(String qString){
		_req.setQuery(qString);
	}
	
	public void clear(){
		_req = new SenseiRequest();
		_req.setOffset(0);
		_req.setCount(5);
		_req.setFetchStoredFields(true);
	}
	
	public void clearSelections(){
		_req.clearSelections();
	}
	
	public SenseiRequest getRequest(){
		return _req;
	}
	
	public String getQueryString(){
		return _req.getQuery();
	}
}
