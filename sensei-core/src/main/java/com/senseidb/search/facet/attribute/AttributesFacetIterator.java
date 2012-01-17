package com.senseidb.search.facet.attribute;

import java.util.Iterator;
import java.util.List;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.FacetIterator;

public class AttributesFacetIterator extends FacetIterator {
  private Iterator<BrowseFacet> iterator;

  public AttributesFacetIterator(List<BrowseFacet> facets) {
    iterator = facets.iterator();
  }

  @Override
  public boolean hasNext() {
    // TODO Auto-generated method stub
    return iterator.hasNext();
  }

  @Override
  public void remove() {
   throw new UnsupportedOperationException();
    
  }

  @Override
  public Comparable next() {
    count = 0;
    BrowseFacet next = iterator.next();
    if (next == null) {
      return null;
    }
    count = next.getFacetValueHitCount();
    facet = next.getValue();
    return next.getValue();
  }

  @Override
  public Comparable next(int minHits) {
    while (iterator.hasNext()) {
      BrowseFacet next = iterator.next();
      count = next.getFacetValueHitCount();
      facet = next.getValue();
      if (next.getFacetValueHitCount() >= minHits) {
        return next.getValue();
      }
    }
    return null;
  }

  @Override
  public String format(Object val) {
    return val != null ? val.toString() : null;
  }
}
