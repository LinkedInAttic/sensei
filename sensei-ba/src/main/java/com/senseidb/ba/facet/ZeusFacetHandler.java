package com.senseidb.ba.facet;

import java.io.IOException;
import java.util.Properties;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreDoc;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.docidset.EmptyDocIdSet;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.FacetCountCollector;
import com.browseengine.bobo.facets.FacetCountCollectorSource;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.facets.impl.DefaultFacetCountCollector;
import com.browseengine.bobo.facets.impl.SimpleFacetHandler;
import com.browseengine.bobo.sort.DocComparator;
import com.browseengine.bobo.sort.DocComparatorSource;
import com.senseidb.ba.ForwardIndex;
import com.senseidb.ba.IndexSegment;

public class ZeusFacetHandler extends FacetHandler<ZeusDataCache> {
  private final String bootsrapFacetHandlerName;
  private final String columnName;

  public ZeusFacetHandler(String name, String columnName, String bootsrapFacetHandlerName) {
    super(name);
    this.columnName = columnName;
    this.bootsrapFacetHandlerName = bootsrapFacetHandlerName;
    
  }

  @Override
  public ZeusDataCache load(BoboIndexReader reader)  {
    if (reader.getFacetData(columnName) != null) {
      return (ZeusDataCache) reader.getFacetData(columnName);
    }
    IndexSegment offlineSegment = (IndexSegment) reader.getFacetData(bootsrapFacetHandlerName);
    return new ZeusDataCache(offlineSegment.getForwardIndex(columnName), offlineSegment.getInvertedIndex(columnName));
    
  }

  @Override
  public RandomAccessFilter buildRandomAccessFilter(final String value, Properties selectionProperty) throws IOException {
    
    
    return new RandomAccessFilter() {
      @Override
      public double getFacetSelectivity(BoboIndexReader reader) {
        final ZeusDataCache zeusDataCache = ZeusFacetHandler.this.load(reader);
        final int index = zeusDataCache.getDictionary().indexOf(value);
        if (index < 0) return 0.0;
        return ((double)zeusDataCache.getForwardIndex().getFrequency(index)) / zeusDataCache.getForwardIndex().getLength();
      }
      @Override
      public RandomAccessDocIdSet getRandomAccessDocIdSet(BoboIndexReader reader) throws IOException {
        final ZeusDataCache zeusDataCache = ZeusFacetHandler.this.load(reader);
        final int index = zeusDataCache.getDictionary().indexOf(value);
        if (index < 0) {
          return EmptyDocIdSet.getInstance();
        }
        //Go by inverted index path
        if (zeusDataCache.invertedIndexPresent(index)) {
           final DocIdSet invertedIndex = zeusDataCache.getInvertedIndexes()[index];
           return new RandomAccessDocIdSet() {
            @Override
            public DocIdSetIterator iterator() throws IOException {
              return invertedIndex.iterator();
            }
            @Override
            public boolean get(int docId) {
              return zeusDataCache.getForwardIndex().getValueIndex(docId) == index;
            }
          };
        }
        else {
          return new ForwardDocIdSet(zeusDataCache.getForwardIndex(), index); 
       }
        
    }};
  }

  @Override
  public FacetCountCollectorSource getFacetCountCollectorSource(final BrowseSelection sel, final FacetSpec fspec) {
   
    return new FacetCountCollectorSource() {
      
      @Override
      public FacetCountCollector getFacetCountCollector(BoboIndexReader reader, int docBase) {
        ZeusDataCache dataCache = load(reader);
        final ForwardIndex forwardIndex = dataCache.getForwardIndex();
        final FacetDataCache fakeCache = dataCache.getFakeCache();
        return new DefaultFacetCountCollector(getName(), dataCache.getFakeCache(), docBase, sel, fspec) {
         
          @Override
          public void collect(int docid) {
            _count[forwardIndex.getValueIndex(docid)]++;
            
          }

          @Override
          public void collectAll() {
            _count = fakeCache.freqs;
            
          }
          
        };
      }
    };
  }
  
  @Override
  public String[] getFieldValues(BoboIndexReader reader, int id) {
    ForwardIndex forwardIndex = load(reader).getForwardIndex();
    return new String[] { forwardIndex.getDictionary().get(forwardIndex.getValueIndex(id))};
  }

  @Override
  public DocComparatorSource getDocComparatorSource() {
    return new DocComparatorSource() {
      @Override
      public DocComparator getComparator(IndexReader reader, int docbase) throws IOException {
        final ZeusDataCache zeusDataCache = ZeusFacetHandler.this.load((BoboIndexReader) reader);
          return new DocComparator() {
          @Override
          public Comparable value(ScoreDoc doc) {
            int index = zeusDataCache.getForwardIndex().getValueIndex(doc.doc);
            return zeusDataCache.getForwardIndex().getDictionary().getComparableValue(index);          
          }
          @Override
          public int compare(ScoreDoc doc1, ScoreDoc doc2) {
            return zeusDataCache.getForwardIndex().getValueIndex(doc2.doc) -zeusDataCache.getForwardIndex().getValueIndex(doc1.doc);
          }
        };
      }
    };
  }
  private static class ForwardIndexIterator extends DocIdSetIterator {
    int doc = -1;
    private final ForwardIndex forwardIndex;
    private final int index;
    public ForwardIndexIterator(ForwardIndex forwardIndex, int index) {
      this.forwardIndex = forwardIndex;
      this.index = index;
    }
    @Override
    public int nextDoc() throws IOException {
      while (true) {
        doc++;
        if (forwardIndex.getLength() <= doc) return NO_MORE_DOCS;
          if (forwardIndex.getValueIndex(doc) == index) {
            return doc;
          }
        }
    }
    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int advance(int target) throws IOException {
      doc = target - 1;
      return nextDoc();
    }
  }
  public static class ForwardDocIdSet extends RandomAccessDocIdSet {
    private ForwardIndex forwardIndex;
    private int index;

    public ForwardDocIdSet(ForwardIndex forwardIndex, int index) {
      this.forwardIndex = forwardIndex;
      this.index = index;
    }
    @Override
    public DocIdSetIterator iterator() throws IOException {
      return new ForwardIndexIterator(forwardIndex, index);
    }
    
    @Override
    public boolean get(int docId) {
      return forwardIndex.getValueIndex(docId) == index;
    }
  }
}
