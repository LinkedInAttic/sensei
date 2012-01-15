package com.senseidb.search.query.filters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.BrowseSelection.ValueOperation;
import com.browseengine.bobo.facets.FacetHandler;
import com.kamikaze.docidset.impl.AndDocIdSet;
import com.senseidb.util.RequestConverter2;

public class FacetSelectionFilterConstructor extends FilterConstructor{
  public static final String FILTER_TYPE = "selection";

  public static BrowseSelection buildFacetSelection(String name,JSONObject json) throws Exception{
    BrowseSelection sel = new BrowseSelection(name);
    String[] vals = RequestConverter2.getStrings(json.optJSONArray(VALUES_PARAM));
    String[] nots = RequestConverter2.getStrings(json.optJSONArray(EXCLUDES_PARAM));
    sel.setValues(vals);
    sel.setNotValues(nots);
    String op = json.optString(OPERATOR_PARAM,OR_PARAM);
    if (OR_PARAM.equalsIgnoreCase(op)){
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
  
  
  @Override
  protected Filter doConstructFilter(Object obj) throws Exception {
    final JSONObject json = (JSONObject)obj;
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
                JSONObject jsonObj = json.getJSONObject(key);
                BrowseSelection sel = buildFacetSelection(key, jsonObj);
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
          throw new IllegalStateException("reader not instance of "+BoboIndexReader.class);
        }
      }
      
    };
    
  }
  
}
