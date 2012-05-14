package com.senseidb.search.facet;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreDoc;

import com.linkedin.zoie.api.DocIDMapper;
import com.linkedin.zoie.api.ZoieIndexReader;
import com.linkedin.zoie.api.ZoieSegmentReader;
import scala.actors.threadpool.Arrays;

import com.linkedin.bobo.api.BoboIndexReader;
import com.linkedin.bobo.api.BrowseSelection;
import com.linkedin.bobo.api.FacetSpec;
import com.linkedin.bobo.docidset.EmptyDocIdSet;
import com.linkedin.bobo.docidset.RandomAccessDocIdSet;
import com.linkedin.bobo.facets.FacetCountCollectorSource;
import com.linkedin.bobo.facets.FacetHandler;
import com.linkedin.bobo.facets.filter.EmptyFilter;
import com.linkedin.bobo.facets.filter.RandomAccessFilter;
import com.linkedin.bobo.facets.filter.RandomAccessNotFilter;
import com.linkedin.bobo.sort.DocComparator;
import com.linkedin.bobo.sort.DocComparatorSource;
import com.kamikaze.docidset.impl.IntArrayDocIdSet;

public class UIDFacetHandler extends FacetHandler<long[]> {
  private static Logger logger = Logger.getLogger(UIDFacetHandler.class);
  public UIDFacetHandler(String name) {
    super(name);
  }
  
  private static class SingleDocRandmAccessDocIdSet extends RandomAccessDocIdSet{
    final int docid;
    SingleDocRandmAccessDocIdSet(int doc){
      docid = doc;
    }
    
    @Override
    public DocIdSetIterator iterator() throws IOException {
      return new DocIdSetIterator() {
        protected int _doc = -1;
        
        @Override
        public int advance(int id) throws IOException {
          _doc = id-1;
          return nextDoc();
        }

        @Override
        public int docID() {
          return _doc;
        }

        @Override
        public int nextDoc() throws IOException {
          if (_doc<docid){
            return _doc = docid;
          }
          return _doc = DocIdSetIterator.NO_MORE_DOCS;
        }
      };
    }
    
    @Override
    public boolean get(int doc) {
      return doc == docid;
    }
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
        ZoieIndexReader<?> zoieReader = (ZoieIndexReader<?>)(reader.getInnerReader());
        DocIDMapper<?> docidMapper = zoieReader.getDocIDMaper();
        
        final int docid = docidMapper.getDocID(val);
        if (docid == DocIDMapper.NOT_FOUND){
          return EmptyDocIdSet.getInstance();
        }
        
        return new SingleDocRandmAccessDocIdSet(docid);
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
        ZoieSegmentReader<?> zoieReader = (ZoieSegmentReader<?>)(reader.getInnerReader());
        DocIDMapper<?> docidMapper = zoieReader.getDocIDMaper();
        
        final IntArrayList docidList = new IntArrayList(valSet.size());
        
        LongIterator iter = valSet.iterator();
        
        while(iter.hasNext()){
          int docid = docidMapper.getDocID(iter.nextLong());
          if (docid!=DocIDMapper.NOT_FOUND){
            docidList.add(docid);
          }
        }
        
        if (docidList.size()==0) return EmptyDocIdSet.getInstance();
        int[] delDocIds = zoieReader.getDelDocIds();
        if (docidList.size()==1) {
          int docId = docidList.getInt(0);
          if (delDocIds == null || delDocIds.length ==0 || Arrays.binarySearch(delDocIds, docId) < 0) {
            return new SingleDocRandmAccessDocIdSet(docidList.getInt(0));
          } else {
            return EmptyDocIdSet.getInstance();
          }         
        }        
        Collections.sort(docidList);        
        final IntArrayDocIdSet intArraySet = new IntArrayDocIdSet(docidList.size());
        boolean deletesPresent = delDocIds != null && delDocIds.length > 0;       
        for (int docid : docidList){
          if (!deletesPresent  || java.util.Arrays.binarySearch(delDocIds,docid) < 0) {
            intArraySet.addDoc(docid);            
          } 
        }        
        return new RandomAccessDocIdSet(){
          @Override
          public boolean get(int docid) {
            return docidList.contains(docid);
          }

          @Override
          public DocIdSetIterator iterator() throws IOException {
            return intArraySet.iterator();
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
    RandomAccessFilter filter;
    if (longSet.size()==1){
      filter = buildRandomAccessFilter(longSet.iterator().nextLong());
    }
    else{
      filter =  buildRandomAccessFilter(longSet);
    }
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

