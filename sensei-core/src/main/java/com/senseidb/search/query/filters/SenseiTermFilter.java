package com.senseidb.search.query.filters;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;

import scala.actors.threadpool.Arrays;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.BrowseSelection.ValueOperation;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.facets.filter.EmptyFilter;
import com.kamikaze.docidset.impl.AndDocIdSet;
import com.kamikaze.docidset.impl.NotDocIdSet;
import com.kamikaze.docidset.impl.OrDocIdSet;

public class SenseiTermFilter extends Filter {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static Logger logger = Logger.getLogger(SenseiTermFilter.class);
  
  private final String _name;
  private final String[] _vals;
  private final String[] _not;
  private final boolean _isAnd;
  private final boolean _noAutoOptimize;
  
  public SenseiTermFilter(String name,String vals[],String[] not,boolean isAnd,boolean noAutoOptimize){
    _name = name;
    _vals = vals;
    _not = not;
    _isAnd = isAnd;
    _noAutoOptimize = noAutoOptimize;
  }
  
  private static DocIdSet buildDefaultDocIdSets(final BoboIndexReader reader,final String name,final String[] vals,boolean isAnd){
    if (vals==null) return null;
    ArrayList<DocIdSet> docSetList = new ArrayList<DocIdSet>(vals.length);
    for (final String val : vals){
      docSetList.add(new DocIdSet() {
        
        @Override
        public DocIdSetIterator iterator() throws IOException {
          return new TermDocIdSetIterator(new Term(name,val), reader);
        }
      });
    }
    
    if (docSetList.size()==1){
      return docSetList.get(0);
    }
    else if (docSetList.size()==0) return null;
    else{
      if (isAnd){
        return new AndDocIdSet(docSetList);
      }
      else{
        return new OrDocIdSet(docSetList);
      }
    }
  }
  
  
  private static DocIdSet buildLuceneDefaultDocIdSet(final BoboIndexReader reader,final String name,final String[] vals,String[] nots,boolean isAnd) throws IOException{
    if (vals!=null && vals.length>0){
      DocIdSet positiveSet = buildDefaultDocIdSets(reader,name,vals,isAnd);
      DocIdSet negativeSet = buildDefaultDocIdSets(reader, name, nots, false);
      
      if (positiveSet!=null){
        if (negativeSet==null){
          return positiveSet;
        }
        else{
          DocIdSet[] sets = new DocIdSet[]{positiveSet,new NotDocIdSet(negativeSet, reader.maxDoc())};
          return new AndDocIdSet(Arrays.asList(sets));
        }
      }
      else{
        if (negativeSet==null){
          return EmptyFilter.getInstance().getDocIdSet(reader);
        }
        else{
          // this could be optimize with AndNot in new Kamikaze
          return new NotDocIdSet(negativeSet, reader.maxDoc());
        }
      }
    }
    else{
      if (logger.isDebugEnabled()){
        logger.debug("null or no term values, returning empty hits");
      }
      return EmptyFilter.getInstance().getRandomAccessDocIdSet(reader);
    }
  }

  @Override
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    if (reader instanceof BoboIndexReader){
      BoboIndexReader boboReader = (BoboIndexReader)reader;
      FacetHandler facetHandler = (FacetHandler)boboReader.getFacetHandler(_name);
      Object obj = null;
      if (facetHandler != null)
      {
        obj = facetHandler.getFacetData(boboReader);
      }
      if (obj!=null && obj instanceof FacetDataCache){
        FacetDataCache facetData = (FacetDataCache)obj;
        TermValueList valArray = facetData.valArray;
        if (_noAutoOptimize){
          // copy vals
          ArrayList<String> validVals = new ArrayList<String>(_vals.length);
          for (String val : _vals){
            int idx = valArray.indexOf(val);
            if (idx>=0){
            validVals.add(valArray.get(idx));    // get and format the value
            }
          }
          return buildLuceneDefaultDocIdSet(boboReader, _name, validVals.toArray(new String[0]),_not,_isAnd);
        }
        // we get to optimize using facets
        BrowseSelection sel = new BrowseSelection(_name);
        sel.setValues(_vals);
        if (_not != null)
          sel.setNotValues(_not);
        if (_isAnd)
          sel.setSelectionOperation(ValueOperation.ValueOperationAnd);
        else
          sel.setSelectionOperation(ValueOperation.ValueOperationOr);
        
        return facetHandler.buildFilter(sel).getDocIdSet(boboReader);
        
      }
      else{
        if (logger.isDebugEnabled()){
          logger.debug("not facet support, default to term filter: "+_name);
        }
        return buildLuceneDefaultDocIdSet(boboReader,_name,_vals,_not,_isAnd);
      }
    }
    else{
      throw new IllegalStateException("read not instance of "+BoboIndexReader.class);
    }
  }

}
