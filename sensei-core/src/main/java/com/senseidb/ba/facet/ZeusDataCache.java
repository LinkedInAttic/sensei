package com.senseidb.ba.facet;

import org.apache.lucene.search.DocIdSet;

import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.ForwardIndex;

public class ZeusDataCache {
  private FacetDataCache fakeCache;
  private DocIdSet[] invertedIndexes;
  private ForwardIndex forwardIndex;
  private TermValueList<?> dictionary;
  public ZeusDataCache(ForwardIndex forwardIndex, DocIdSet[] invertedIndexes) {
    this.forwardIndex = forwardIndex;
    this.invertedIndexes = invertedIndexes;
    dictionary = forwardIndex.getDictionary();
    fakeCache = createFakeFacetDataCache(forwardIndex);
  }
  
  public boolean invertedIndexPresent(int dictionaryIndex) {
    return invertedIndexes != null && dictionaryIndex < invertedIndexes.length && invertedIndexes[dictionaryIndex] != null;
  }
  public static FacetDataCache createFakeFacetDataCache(ForwardIndex forwardIndex) {
    FacetDataCache newDataCache = new FacetDataCache<String>();
    newDataCache.valArray = forwardIndex.getDictionary(); 
    newDataCache.freqs =  new int[forwardIndex.getDictionary().size()];
    for (int i = 0 ; i < forwardIndex.getDictionary().size(); i++) {
      newDataCache.freqs[i] = forwardIndex.getFrequency(i);
    }    
    return newDataCache;
  }
  
  public FacetDataCache getFakeCache() {
    return fakeCache;
  }
  public void setFakeCache(FacetDataCache fakeCache) {
    this.fakeCache = fakeCache;
  }
  public DocIdSet[] getInvertedIndexes() {
    return invertedIndexes;
  }
  public void setInvertedIndexes(DocIdSet[] invertedIndexes) {
    this.invertedIndexes = invertedIndexes;
  }

  public ForwardIndex getForwardIndex() {
    return forwardIndex;
  }

  public void setForwardIndex(ForwardIndex forwardIndex) {
    this.forwardIndex = forwardIndex;
  }

  public TermValueList<?> getDictionary() {
    return dictionary;
  }

  public void setDictionary(TermValueList<?> dictionary) {
    this.dictionary = dictionary;
  }
  
}
