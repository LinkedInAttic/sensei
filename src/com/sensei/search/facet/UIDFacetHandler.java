package com.sensei.search.facet;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreDoc;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieSegmentReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.FacetCountCollectorSource;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.filter.EmptyFilter;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.facets.filter.RandomAccessNotFilter;
import com.browseengine.bobo.sort.DocComparator;
import com.browseengine.bobo.sort.DocComparatorSource;

public class UIDFacetHandler extends FacetHandler<long[]> {

  public UIDFacetHandler(String name) {
    super(name);
  }
  
  private RandomAccessFilter buildRandomAccessFilter(final long val) throws IOException {
    return new RandomAccessFilter() {
      
      /**
       * 
       */
      private static final long serialVersionUID = 1L;

      @Override
      public RandomAccessDocIdSet getRandomAccessDocIdSet(BoboIndexReader reader)
          throws IOException {
       
        final long[] uidArray = UIDFacetHandler.this.getFacetData(reader);
        
        int[] delDocArray = ((ZoieIndexReader<?>)(reader.getInnerReader())).getDelDocIds();
        
		if (delDocArray!=null){
          Arrays.sort(delDocArray);
        }
        else{
          delDocArray = new int[0];
        }
        
        final int[] delDocs = delDocArray;
        
        return new RandomAccessDocIdSet() {
          
          @Override
          public DocIdSetIterator iterator() throws IOException {
            return new DocIdSetIterator() {
              protected int _doc = -1;
              private int _len = uidArray.length; 
              
              private int _idxInDelDocs = 0;
              private int _lenDelDocs = delDocs.length;
              
              @Override
              public int advance(int id) throws IOException {
                if (_doc < id){
                      _doc = id - 1;
                    }
                  
                while(++_doc < _len) // not yet reached end
                {
                  // check if the docId was deleted
                  while(_idxInDelDocs<_lenDelDocs && delDocs[_idxInDelDocs] < _doc)
                  {
                    ++_idxInDelDocs;
                  }
                  if(_idxInDelDocs < _lenDelDocs && delDocs[_idxInDelDocs] == _doc)
                  {
                    ++_idxInDelDocs;
                    continue;
                  }
                  
                  if (uidArray[_doc] == val){
                    return _doc;
                  }
                }
                return _doc=DocIdSetIterator.NO_MORE_DOCS;
              }

              @Override
              public int docID() {
                return _doc;
              }

              @Override
              public int nextDoc() throws IOException {
                while(++_doc < _len) // not yet reached end
                {
                    // check if the docId was deleted
                    while(_idxInDelDocs<_lenDelDocs && delDocs[_idxInDelDocs] < _doc)
                    {
                      ++_idxInDelDocs;
                    }
                    if(_idxInDelDocs < _lenDelDocs && delDocs[_idxInDelDocs] == _doc)
                    {
                      ++_idxInDelDocs;
                      continue;
                    }
                    
                    if (uidArray[_doc] == val){
                    return _doc;
                  }
                }
                return _doc = DocIdSetIterator.NO_MORE_DOCS;
              }
            };
          }
          
          @Override
          public boolean get(int docId) {
            return (Arrays.binarySearch(delDocs, docId)<0) && val == uidArray[docId];
          }
        };
      }
    };
  }
  
  private RandomAccessFilter buildRandomAccessFilter(final LongSet valSet) throws IOException {
        return new RandomAccessFilter() {
      
      /**
       * 
       */
      private static final long serialVersionUID = 1L;

      @Override
      public RandomAccessDocIdSet getRandomAccessDocIdSet(BoboIndexReader reader)
          throws IOException {
        final long[] uidArray = UIDFacetHandler.this.getFacetData(reader);
        
        int[] delDocArray = ((ZoieIndexReader<?>)(reader.getInnerReader())).getDelDocIds();
        if (delDocArray!=null){
          Arrays.sort(delDocArray);
        }
        else{
        	delDocArray = new int[0];
        }
        
        final int[] delDocs = delDocArray;
        
        return new RandomAccessDocIdSet() {
          
          @Override
          public DocIdSetIterator iterator() throws IOException {
            return new DocIdSetIterator() {
              protected int _doc = -1;
              private int _len = uidArray.length; 
              private int _idxInDelDocs = 0;
              private int _lenDelDocs = delDocs.length;
              
              @Override
              public int advance(int id) throws IOException {
                if (_doc < id){
                      _doc = id - 1;
                    }
                
                while(++_doc < _len) // not yet reached end
                {
                  // check if the docId was deleted
                  while(_idxInDelDocs<_lenDelDocs && delDocs[_idxInDelDocs] < _doc)
                  {
                    ++_idxInDelDocs;
                  }
                  if(_idxInDelDocs < _lenDelDocs && delDocs[_idxInDelDocs] == _doc)
                  {
                    ++_idxInDelDocs;
                    continue;
                  }
                  
                  if (valSet.contains(uidArray[_doc])){
                    return _doc;
                  }
                }
                return _doc=DocIdSetIterator.NO_MORE_DOCS;
              }

              @Override
              public int docID() {
                return _doc;
              }

              @Override
              public int nextDoc() throws IOException {
                while(++_doc < _len) // not yet reached end
                {
               // check if the docId was deleted
                  while(_idxInDelDocs<_lenDelDocs && delDocs[_idxInDelDocs] < _doc)
                  {
                    ++_idxInDelDocs;
                  }
                  if(_idxInDelDocs < _lenDelDocs && delDocs[_idxInDelDocs] == _doc)
                  {
                    ++_idxInDelDocs;
                    continue;
                  }
                  
                  if (valSet.contains(uidArray[_doc])){
                    return _doc;
                  }
                }
                return _doc = DocIdSetIterator.NO_MORE_DOCS;
              }
            };
          }
          
          
          @Override
          public boolean get(int docId) {
            return (Arrays.binarySearch(delDocs, docId)<0) && valSet.contains(uidArray[docId]);
          }
        };
      }
    };
  }
  
  @Override
  public RandomAccessFilter buildRandomAccessFilter(String value,
      Properties selectionProperty) throws IOException {
    try{
      long val = Long.parseLong(value);
      return buildRandomAccessFilter(val);
    }
    catch(Exception e){
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public RandomAccessFilter buildRandomAccessAndFilter(String[] vals,
      Properties prop) throws IOException {
    LongSet longSet = new LongOpenHashSet();
    for (String val : vals){
      try{
        longSet.add(Long.parseLong(val));
      }
      catch(Exception e){
        throw new IOException(e.getMessage());
      }
    }
    if (longSet.size()!=1){
      return EmptyFilter.getInstance();
    }
    else{
      return buildRandomAccessFilter(longSet.iterator().nextLong());
    }
  }

  @Override
  public RandomAccessFilter buildRandomAccessOrFilter(String[] vals,
      Properties prop, boolean isNot) throws IOException {
    LongSet longSet = new LongOpenHashSet();
    for (String val : vals){
      try{
        longSet.add(Long.parseLong(val));
      }
      catch(Exception e){
        throw new IOException(e.getMessage());
      }
    }
    RandomAccessFilter filter =  buildRandomAccessFilter(longSet);
    if (filter == null) return filter;
    if (isNot)
    {
      filter = new RandomAccessNotFilter(filter);
    }
    return filter;
  }

  @Override
  public DocComparatorSource getDocComparatorSource() {
    return new DocComparatorSource() {
      
      @Override
      public DocComparator getComparator(IndexReader reader, int docbase)
          throws IOException {
        final UIDFacetHandler uidFacetHandler = UIDFacetHandler.this;
        if (reader instanceof BoboIndexReader){
          BoboIndexReader boboReader = (BoboIndexReader)reader;
          final long[] uidArray = uidFacetHandler.getFacetData(boboReader);
          return new DocComparator() {
            
            @Override
            public Comparable value(ScoreDoc doc) {
              int docid = doc.doc;
              return Long.valueOf(uidArray[docid]);
            }
            
            @Override
            public int compare(ScoreDoc doc1, ScoreDoc doc2) {
              long uid1 = uidArray[doc1.doc];
              long uid2 = uidArray[doc2.doc];
              if (uid1==uid2){
                return 0;
              }
              else{
                if (uid1<uid2) return -1;
                return 1;
              }
            }
          };
        }
        else{
          throw new IOException("reader must be instance of: "+BoboIndexReader.class);
        }
      }
    };
  }

  @Override
  public FacetCountCollectorSource getFacetCountCollectorSource(
      BrowseSelection sel, FacetSpec fspec) {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public String[] getFieldValues(BoboIndexReader reader, int id) {
    long[] uidArray = getFacetData(reader);
    return new String[]{String.valueOf(uidArray[id])};
  }
  

  @Override
  public Object[] getRawFieldValues(BoboIndexReader reader, int id) {
    long[] uidArray = getFacetData(reader);
    return new Long[]{uidArray[id]};
  }

  @Override
  public long[] load(BoboIndexReader reader) throws IOException {
    IndexReader innerReader = reader.getInnerReader();
    if (innerReader instanceof ZoieSegmentReader){
      ZoieSegmentReader zoieReader = (ZoieSegmentReader)innerReader;
      return zoieReader.getUIDArray();
    }
    else{
      throw new IOException("inner reader not instance of "+ZoieSegmentReader.class);
    }
  }

}

