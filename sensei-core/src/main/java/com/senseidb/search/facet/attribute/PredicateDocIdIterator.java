package com.senseidb.search.facet.attribute;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;

import com.browseengine.bobo.facets.data.FacetDataCache;
@SuppressWarnings("rawtypes")
public class PredicateDocIdIterator extends DocIdSetIterator {
  private final int startDocId;
  private final int endDocId;
  private final FacetPredicate facetPredicate;
  private int docId = -1;
  private final FacetDataCache facetDataCache;
  
  
  public PredicateDocIdIterator(int startDocId, int endDocId, FacetPredicate facetPredicate, FacetDataCache facetDataCache) {
    
    this.startDocId = startDocId;
    this.endDocId = endDocId;
    if (startDocId > endDocId) {
      throw new IllegalStateException("startID shouldn't be greater than endId");
    }
    this.facetPredicate = facetPredicate;
    this.facetDataCache = facetDataCache;
  }
  
  @Override
  public int docID() {    
    return docId;
  }
  
  @Override
  public int nextDoc() throws IOException {
    if (docId == -1) {
      docId = startDocId - 1;
    }
    if (docId == NO_MORE_DOCS) {
      return docId;
    }
    docId++;
    while (!facetPredicate.evaluate(facetDataCache, docId) || docId > endDocId) {      
      if (docId > endDocId) {
        docId = NO_MORE_DOCS;
        return docId;
      }
      docId++;
    }
    return docId;
  }

  @Override
  public int advance(int target) throws IOException {
    if (target > endDocId) {
      return NO_MORE_DOCS;      
    }
    docId = target - 1;
    return nextDoc();
  }
}
