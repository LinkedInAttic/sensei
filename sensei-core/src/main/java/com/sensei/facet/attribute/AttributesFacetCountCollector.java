package com.sensei.facet.attribute;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetIterator;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.impl.DefaultFacetCountCollector;
import com.browseengine.bobo.util.BigNestedIntArray;

public  final class AttributesFacetCountCollector extends DefaultFacetCountCollector {
  private final AttributesFacetHandler attributesFacetHandler;
  public final BigNestedIntArray _array;
  private RangePredicate rangePredicate;
  private int[] buffer;   
  private List<BrowseFacet> cachedFacets;
  private final int numFacetsPerKey;
  private final char separator;
  @SuppressWarnings("rawtypes")
  public AttributesFacetCountCollector(AttributesFacetHandler attributesFacetHandler, String name, MultiValueFacetDataCache dataCache, int docBase, BrowseSelection browseSelection, FacetSpec ospec, int numFacetsPerKey, char separator){
    super(name,dataCache,docBase,browseSelection,ospec);
    this.attributesFacetHandler = attributesFacetHandler;
    this.numFacetsPerKey = numFacetsPerKey;
    this.separator = separator;
    _array = dataCache._nestedArray;
     buffer = new int[10];
    if (browseSelection != null) {
      rangePredicate = new RangePredicate(browseSelection.getValues(), browseSelection.getNotValues(), browseSelection.getSelectionOperation(), separator);
    }
  }

  @Override
  public final void collect(int docid) {
    if (rangePredicate != null) {
      if (buffer.length < _array.getNumItems(docid)) {
        buffer = new int[_array.getNumItems(docid) + 10];
      }
     int count = _array.getData(docid, buffer);
     if (count == 1) {
      if (rangePredicate.evaluateValue(_dataCache, buffer[0])) {
        _count[buffer[0]]++;
      }
     }
     for (int i = 0; i < count; i++) {
       if (rangePredicate.evaluateValue(_dataCache, buffer[i])) {
         _count[buffer[i]]++;
       }
     }        
    } else {
      _array.countNoReturn(docid, _count);
    }
  }

  @Override
  public final void collectAll()
  {
    _count = _dataCache.freqs;
  }
  @Override
  public List<BrowseFacet> getFacets() {
    if (cachedFacets == null) {
    int max = _ospec.getMaxCount();
    _ospec.setMaxCount(-1);
    List<BrowseFacet> facets = super.getFacets();
    _ospec.setMaxCount(max);
    filterByKeys(facets,  separator, numFacetsPerKey);
    cachedFacets = facets;
    }
    return cachedFacets;
  }
  
  private void filterByKeys(List<BrowseFacet> facets, char separator, int numFacetsPerKey) {
    Map<String, AtomicInteger> keyOccurences = new HashMap<String, AtomicInteger>();
    Iterator<BrowseFacet> iterator = facets.iterator();
    String separatorString = String.valueOf(separator);
    while (iterator.hasNext()) {
      BrowseFacet facet = iterator.next();
      String value = facet.getValue();
      if (!value.contains(separatorString)) {
        iterator.remove();
        continue;
      }
      String key = value.substring(0, value.indexOf(separatorString));
      AtomicInteger numOfKeys = keyOccurences.get(key);
      if (numOfKeys == null) {
        numOfKeys = new AtomicInteger(0);
        keyOccurences.put(key, numOfKeys);
      }
      int count = numOfKeys.incrementAndGet();
      if (count > numFacetsPerKey) {
        iterator.remove();
      }
    }
    
  }

  @Override
  public FacetIterator iterator() {    
    return new AttributesFacetIterator(getFacets());
  }
  
}