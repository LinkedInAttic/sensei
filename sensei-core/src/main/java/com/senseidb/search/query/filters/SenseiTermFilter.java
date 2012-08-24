package com.senseidb.search.query.filters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.BrowseSelection.ValueOperation;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.facets.filter.EmptyFilter;
import com.browseengine.bobo.query.MatchAllDocIdSetIterator;
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
    _vals = vals != null  ? vals : new String[0];
    _not = not != null  ? not : new String[0];
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
  
  
  private static DocIdSet buildLuceneDefaultDocIdSet(final BoboIndexReader reader,
                                                     final String name,
                                                     final String[] vals,
                                                     String[] nots,
                                                     boolean isAnd) throws IOException{
    if (reader.getRuntimeFacetHandlerFactoryMap().containsKey(name))
    {
      // Skip runtime facet handlers
      return new DocIdSet()
      {
        @Override
        public boolean isCacheable()
        {
          return false;
        }

        @Override
        public DocIdSetIterator iterator() throws IOException
        {
          return new MatchAllDocIdSetIterator(reader);
        }
      };
    }
    DocIdSet positiveSet = null;
    DocIdSet negativeSet = null;

    if (vals!=null && vals.length>0)
      positiveSet = buildDefaultDocIdSets(reader,name,vals,isAnd);

    if (nots!=null && nots.length>0)
      negativeSet = buildDefaultDocIdSets(reader, name, nots, false);

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
        return EmptyFilter.getInstance().getRandomAccessDocIdSet(reader);
      }
      else{
        // this could be optimize with AndNot in new Kamikaze
        return new NotDocIdSet(negativeSet, reader.maxDoc());
      }
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
        if (_noAutoOptimize && obj!=null && obj instanceof FacetDataCache){
          FacetDataCache facetData = (FacetDataCache)obj;
          TermValueList valArray = facetData.valArray;
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

        Filter filter = facetHandler.buildFilter(sel);
        if (filter == null)
          filter = EmptyFilter.getInstance();

        return filter.getDocIdSet(boboReader);
        
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
