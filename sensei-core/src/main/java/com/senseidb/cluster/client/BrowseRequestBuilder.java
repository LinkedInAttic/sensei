package com.senseidb.cluster.client;

import java.io.UnsupportedEncodingException;

import org.apache.lucene.search.SortField;
import org.json.JSONObject;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.senseidb.search.req.SenseiJSONQuery;
import com.senseidb.search.req.SenseiQuery;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;

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
		JSONObject qObj = new FastJSONObject();
		if (qString!=null){
			try {
				qObj.put("query", qString);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		_req.setQuery(new SenseiJSONQuery(qObj));
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
		SenseiQuery q = _req.getQuery();
		if (q!=null){
			try {
				return new String(q.toBytes(),"UTF-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
				return null;
			}
		}
		return null;
	}
}
