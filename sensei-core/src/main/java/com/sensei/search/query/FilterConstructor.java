package com.sensei.search.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.json.JSONArray;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.BrowseSelection.ValueOperation;
import com.browseengine.bobo.facets.FacetHandler;
import com.kamikaze.docidset.impl.AndDocIdSet;
import com.sensei.conf.SenseiFacetHandlerBuilder;
import com.sensei.search.facet.UIDFacetHandler;
import com.sensei.search.util.RequestConverter2;

public abstract class FilterConstructor {

	public static final String VALUES_PARAM = "values";
	public static final String EXCLUDES_PARAM = "excludes";
	public static final String OPERATOR_PARAM = "operator";
	public static final String PARAMS_PARAM = "params";
	public static final String MUST_PARAM = "must";
	public static final String MUST_NOT_PARAM = "must_not";
	public static final String SHOULD_PARAM = "should";
	
	public static FilterConstructor getFilterConstructor(String type){
		return null;
	}
	
	public static Map<String,String> convertParams(JSONObject obj){
		Map<String,String> paramMap = new HashMap<String,String>();
		String[] names = JSONObject.getNames(obj);
		if (names!=null){
		  for (String name:names){
			String val = obj.optString(name, null);
			if (val!=null){
				paramMap.put(name, val);
			}
		  }
		}
		return paramMap;
	}
	
	public static class BoolFilterConstructor extends FilterConstructor{

		@Override
		public Filter constructFilter(JSONObject json) throws Exception {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	
	public static class AndFilterConstructor extends FilterConstructor{

		@Override
		public Filter constructFilter(JSONObject json) throws Exception {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	
	public static class OrFilterConstructor extends FilterConstructor{

		@Override
		public Filter constructFilter(JSONObject json) throws Exception {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	
	public static class RangeFilterConstructor extends FilterConstructor{

		@Override
		public Filter constructFilter(JSONObject json) throws Exception {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	
	public static class PathFilterConstructor extends FilterConstructor{

		@Override
		public Filter constructFilter(JSONObject json) throws Exception {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	
	
	abstract public Filter constructFilter(JSONObject json) throws Exception;
	
	public static class TermFilterConstructor extends FilterConstructor{

		@Override
		public Filter constructFilter(JSONObject json) throws Exception {
			boolean noOptimize = json.optBoolean("_noOptimize",false);
			
			String[] names = JSONObject.getNames(json);
			String termName = null;
			for (String name : names){
				if (!name.equals("_noOptimize")){
					termName = name;
					break;
				}
			}
			
			if (termName == null) throw new IllegalArgumentException("no term name specified: "+json);
			String val = json.optString(termName, null);
			if (val==null)  throw new IllegalArgumentException("no term value specified: "+json);
			return new SenseiTermFilter(termName, new String[]{val}, null, false, noOptimize);
		}
		
	}
	
	public static class TermsFilterConstructor extends FilterConstructor{
		@Override
		public Filter constructFilter(JSONObject json) throws Exception {
			boolean noOptimize = json.optBoolean("_noOptimize",false);
			
			String[] names = JSONObject.getNames(json);
			String termName = null;
			for (String name : names){
				if (!name.equals("_noOptimize")){
					termName = name;
					break;
				}
			}
			
			if (termName == null) throw new IllegalArgumentException("no term name specified: "+json);
			
			Object obj = json.opt(termName);
			if (obj == null){
				throw new IllegalArgumentException("no term value specified: "+json);
			}
			if (obj instanceof JSONArray){
				JSONArray jsonArray = (JSONArray)obj;
				String[] vals = RequestConverter2.getStrings(jsonArray);
				return new SenseiTermFilter(termName, vals, null, false, noOptimize);	
			}
			else if (obj instanceof JSONObject){
				JSONObject jsonObj = (JSONObject)obj;
				String[] vals = RequestConverter2.getStrings(jsonObj, "values");
				String[] notVals = RequestConverter2.getStrings(jsonObj, "excludes");
				String op = jsonObj.optString("operator","or");
				boolean isAnd = false;
				if (!"or".equals(op)){
					isAnd = true;
				}
				return new SenseiTermFilter(termName, vals, notVals, isAnd, noOptimize);
			}
			else{
				throw new IllegalArgumentException("invalid term value specified: "+json);
			}
		}
	}
	
	public static class QueryFilterConstructor extends FilterConstructor{

		@Override
		public Filter constructFilter(JSONObject json) throws Exception {
			JSONObject queryObj = json.getJSONObject("query");
			String type = (String)queryObj.keys().next();
			QueryConstructor qconstructor = QueryConstructor.getQueryConstructor(type);
			if (qconstructor == null){
				throw new IllegalArgumentException("unknow query type: "+type);
			}
			Query q = qconstructor.constructQuery(queryObj.optJSONObject(type));
			return new QueryWrapperFilter(q);
		}
		
	}
	
	public static class UIDFilterConstructor extends FilterConstructor{

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
	
	public static BrowseSelection buildFacetSelection(String name,JSONObject json) throws Exception{
		BrowseSelection sel = new BrowseSelection(name);
		String[] vals = RequestConverter2.getStrings(json.optJSONArray(VALUES_PARAM));
		String[] nots = RequestConverter2.getStrings(json.optJSONArray(EXCLUDES_PARAM));
		sel.setValues(vals);
		sel.setNotValues(nots);
		String operator = json.optString(OPERATOR_PARAM,"or");
		if ("or".equalsIgnoreCase(operator)){
			sel.setSelectionOperation(ValueOperation.ValueOperationOr);
		}
		else{
			sel.setSelectionOperation(ValueOperation.ValueOperationAnd);
		}
		JSONObject paramsObj = json.optJSONObject(PARAMS_PARAM);
		if (paramsObj!=null){
			Map<String,String> paramMap = convertParams(paramsObj);
			if (paramMap!=null && paramMap.size()>0){
		 	  sel.setSelectionProperties(paramMap);
			}
		}
		return sel;
	}
	
	public static class FacetSelectionFilterConstructor extends FilterConstructor{

		@Override
		public Filter constructFilter(final JSONObject json) throws Exception {
			return new Filter(){

				@Override
				public DocIdSet getDocIdSet(IndexReader reader)
						throws IOException {
					if (reader instanceof BoboIndexReader){
						BoboIndexReader boboReader = (BoboIndexReader)reader;
						Iterator<String> iter = json.keys();
						ArrayList<DocIdSet> docSets = new ArrayList<DocIdSet>();
						while(iter.hasNext()){
							String key = iter.next();
							FacetHandler facetHandler = boboReader.getFacetHandler(key);
							if (facetHandler!=null){
							  try{
						 	    JSONObject obj = json.getJSONObject(key);
							    BrowseSelection sel = buildFacetSelection(key, obj);
							    docSets.add(facetHandler.buildFilter(sel).getDocIdSet(boboReader));
							  }
							  catch(Exception e){
								  throw new IOException(e.getMessage());
							  }
							}
							else{
							  throw new IOException(key+" is not defined as a facet handler");
							}
						}
						if (docSets.size()==0) return null;
						else if (docSets.size()==1) return docSets.get(0);
						return new AndDocIdSet(docSets);
					}
					else{
						throw new IllegalStateException("read not instance of "+BoboIndexReader.class);
					}
				}
				
			};
			
		}
		
	}
}
