package com.sensei.search.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.lucene.search.SortField;
import org.json.JSONArray;
import org.json.JSONObject;

import com.browseengine.bobo.api.FacetSpec;
import com.sensei.search.req.SenseiRequest;

public class RequestConverter2 {

	private static String[] getStrings(JSONObject obj,String field){
		  String[] strArray = null;
		  JSONArray array = obj.optJSONArray(field);
		  if (array!=null){
			int count = array.length();
			strArray = new String[count];
			for (int i=0;i<count;++i){
				strArray[i] = array.optString(i);
			}
		  }
		  return strArray;
	  }
	  
	  private static int[] getInts(JSONObject obj,String field,int defaultVal){
		  int[] intArray = null;
		  JSONArray array = obj.optJSONArray(field);
		  if (array!=null){
			int count = array.length();
			intArray = new int[count];
			for (int i=0;i<count;++i){
				intArray[i] = array.optInt(i,defaultVal);
			}
		  }
		  return intArray;
	  }
	  
	  private static Set<Integer> getIntSet(JSONObject obj,String field,int defaultVal){
		  HashSet<Integer> intSet = null;
		  JSONArray array = obj.optJSONArray(field);
		  if (array!=null){
			int count = array.length();
			intSet = new HashSet<Integer>(count);
			for (int i=0;i<count;++i){
				intSet.add(array.optInt(i,defaultVal));
			}
		  }
		  return intSet;
	  }
	  
	public static SenseiRequest fromJSON(JSONObject json) throws Exception{
		SenseiRequest req = new SenseiRequest();
		
		// paging params
		int offset = json.optInt("from", 0);
		int count = json.optInt("size",10);
		
		req.setOffset(offset);
		req.setCount(count);
		
		// group by params
		JSONObject groupBy = json.optJSONObject("groupBy");
		if (groupBy!=null){
		  req.setGroupBy(groupBy.optString("column", null));
		  req.setMaxPerGroup(groupBy.optInt("top", 3));
		}
		
		 // facets
		  
		  JSONObject facets = json.optJSONObject("facets");
		  if (facets!=null){
			  Iterator<String> keyIter = facets.keys();
			  while (keyIter.hasNext()){
				  String field = keyIter.next();
				  JSONObject facetObj = facets.getJSONObject(field);
				  if (facetObj!=null){
					 FacetSpec facetSpec = new FacetSpec();
					 facetSpec.setMaxCount(facetObj.optInt("max", 10));
					 facetSpec.setMinHitCount(facetObj.optInt("minCount", 1));
					 facetSpec.setExpandSelection(facetObj.optBoolean("expand", false));
					 
					 String orderBy = facetObj.optString("order", "hits");
					 FacetSpec.FacetSortSpec facetOrder = FacetSpec.FacetSortSpec.OrderHitsDesc;
					 if ("val".equals(orderBy)){
						 facetOrder = FacetSpec.FacetSortSpec.OrderValueAsc;
					 }
					 
					 facetSpec.setOrderBy(facetOrder);
					 req.setFacetSpec(field, facetSpec);
				  }
			  }
		  }
		// sorts
		  
		  JSONArray sortArray = json.optJSONArray("sort");
		  if (sortArray!=null && sortArray.length()>0){
			  ArrayList<SortField> sortFieldList = new ArrayList<SortField>(sortArray.length());
			  for (int i=0;i<sortArray.length();++i){
				String strForm = sortArray.optString(i, null);
				if (strForm!=null && "_score".equals(strForm)){
					sortFieldList.add(SortField.FIELD_SCORE);
			    	continue;
				}
				if (sortArray.optString(i,null)==null){
					
				}
			    JSONObject sortObj = sortArray.optJSONObject(i);
			    if (sortObj!=null){
			       String[] fieldNames = JSONObject.getNames(sortObj);
			       if (fieldNames!=null && fieldNames.length>0){
			    	   String field = fieldNames[0];
			    	   boolean reverse=false;
			    	   if ("desc".equals(sortObj.optString(field, "asc"))){
			    		   reverse = true;
			    	   }
			    	   sortFieldList.add(new SortField(field,SortField.CUSTOM,reverse));
			       }
			    }
			  }
			  if (sortFieldList.size()>0){
			    req.setSort(sortFieldList.toArray(new SortField[0]));
			  }
		  }
		
		// other
		  
		boolean fetchStored = json.optBoolean("stored");
		req.setFetchStoredFields(fetchStored);
		  
		String[] termVectors = getStrings(json,"fetchTermVectors");
		if (termVectors!=null && termVectors.length>0){
		  req.setTermVectorsToFetch(new HashSet<String>(Arrays.asList(termVectors)));
		}
		  

		req.setPartitions(getIntSet(json,"partitions",0));
		  
		req.setShowExplanation(json.optBoolean("explain",false));
		  
		req.setRouteParam(json.optString("routeParam",null));
		  
		return req;
	}
}
