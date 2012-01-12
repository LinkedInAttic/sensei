package com.sensei.facet.attribute;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.lucene.util.OpenBitSet;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.FacetCountCollector;
import com.browseengine.bobo.facets.FacetCountCollectorSource;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.data.TermListFactory;
import com.browseengine.bobo.facets.filter.EmptyFilter;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.facets.filter.RandomAccessNotFilter;
import com.browseengine.bobo.facets.impl.MultiValueFacetHandler;

public class AttributesFacetHandler extends MultiValueFacetHandler {
  public static final char DEFAULT_SEPARATOR = '=';
  private char separator;
  private int numFacetsPerKey = 7;
  public static final String SEPARATOR_PROP_NAME = "separator";
  public static final String MAX_FACETS_PER_KEY_PROP_NAME = "maxFacetsPerKey";
  
  public AttributesFacetHandler(String name, String indexFieldName, TermListFactory termListFactory, Term sizePayloadTerm, Set<String> depends, Map<String, String> facetProps) {
    super(name, indexFieldName, termListFactory, sizePayloadTerm, depends);
    if (facetProps.containsKey(SEPARATOR_PROP_NAME)) {
      this.separator = narrow(facetProps.get(SEPARATOR_PROP_NAME)).charAt(0); 
    } else {
      this.separator = DEFAULT_SEPARATOR;
    }
    if (facetProps.containsKey(MAX_FACETS_PER_KEY_PROP_NAME)) {
      this.numFacetsPerKey = Integer.parseInt(narrow(facetProps.get(MAX_FACETS_PER_KEY_PROP_NAME))); 
    }
  }
  private String narrow(String string) {   
    return string.replaceAll("\\[", "").replaceAll("\\]", "");
  }
  public char getSeparator(BrowseSelection browseSelection) {
    if (browseSelection == null || !browseSelection.getSelectionProperties().containsKey(SEPARATOR_PROP_NAME)) {
      return separator;
    }
    return browseSelection.getSelectionProperties().get(SEPARATOR_PROP_NAME).toString().charAt(0);
  }
  
  @Override
  public RandomAccessFilter buildRandomAccessFilter(String value, Properties prop) throws IOException {    
    return new PredicateFacetFilter(new SimpleDataCacheBuilder(getName()), new RangePredicate(value, separator));
  }
  
  @Override
  public RandomAccessFilter buildRandomAccessOrFilter(final String[] vals, Properties prop, boolean isNot) throws IOException {
    if (vals.length == 0) {
      return EmptyFilter.getInstance();
    }
    RandomAccessFilter filter;
    if (vals.length == 1) {
      filter = buildRandomAccessFilter(vals[0], prop);
    } else {   
       filter =  new BitSetFilter(new BitSetBuilder() {
        @Override
        public OpenBitSet bitSet(FacetDataCache dataCache) {
          return buildBitSet(dataCache, vals);
        }
      }, new SimpleDataCacheBuilder(getName()));
    }
    if (!isNot) {
      return filter;
    } else {
      return new RandomAccessNotFilter(filter);
    }
  }
  
  
  
  public int getFacetsPerKey(BrowseSelection browseSelection) {
    if (browseSelection == null || !browseSelection.getSelectionProperties().containsKey(MAX_FACETS_PER_KEY_PROP_NAME)) {
      return numFacetsPerKey;
    }
    return Integer.valueOf(browseSelection.getSelectionProperties().get(MAX_FACETS_PER_KEY_PROP_NAME).toString());
  }
  
  public OpenBitSet buildBitSet(FacetDataCache facetDataCache, String[] values) {
    MultiValueFacetDataCache multiCache = (MultiValueFacetDataCache) facetDataCache;
    Range[] includes = Range.getRanges(multiCache, values, separator);
    int max = -1;
    
    OpenBitSet openBitSet = new OpenBitSet(facetDataCache.valArray.size()); 
    for(Range range : includes) {
      for (int i = range.start; i < range.end; i++) {
        openBitSet.fastSet(i);
      }
    }
    return openBitSet;
  }
  @Override
  public FacetCountCollectorSource getFacetCountCollectorSource(final BrowseSelection browseSelection, final FacetSpec ospec){
   
    return new FacetCountCollectorSource(){
    
    @Override
    public FacetCountCollector getFacetCountCollector(
        BoboIndexReader reader, int docBase) {
      int facetsPerKey = getFacetsPerKey(browseSelection);
      if (ospec.getProperties() != null && ospec.getProperties().containsKey(MAX_FACETS_PER_KEY_PROP_NAME)) {
        facetsPerKey = Integer.parseInt(ospec.getProperties().get(MAX_FACETS_PER_KEY_PROP_NAME));
      }
      MultiValueFacetDataCache dataCache = (MultiValueFacetDataCache) reader.getFacetData(_name);
      return new AttributesFacetCountCollector(AttributesFacetHandler.this, _name,dataCache,docBase,browseSelection, ospec, facetsPerKey, getSeparator(browseSelection));
    }
  };
  }
}
