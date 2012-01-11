package com.sensei.facet.attribute;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.Term;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.BrowseSelection.ValueOperation;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.FacetCountCollector;
import com.browseengine.bobo.facets.FacetCountCollectorSource;
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
  public static final String NUM_FACETS_PER_KEY_PROP_NAME = "numFacetsPerKey";
  
  public AttributesFacetHandler(String name, String indexFieldName, TermListFactory termListFactory, Term sizePayloadTerm, Set<String> depends, Map<String, String> facetProps) {
    super(name, indexFieldName, termListFactory, sizePayloadTerm, depends);
    if (facetProps.containsKey(SEPARATOR_PROP_NAME)) {
      this.separator = narrow(facetProps.get(SEPARATOR_PROP_NAME)).charAt(0); 
    } else {
      this.separator = DEFAULT_SEPARATOR;
    }
    if (facetProps.containsKey(NUM_FACETS_PER_KEY_PROP_NAME)) {
      this.numFacetsPerKey = Integer.parseInt(narrow(facetProps.get(NUM_FACETS_PER_KEY_PROP_NAME))); 
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
  public int getFacetsPerKey(BrowseSelection browseSelection) {
    if (browseSelection == null || !browseSelection.getSelectionProperties().containsKey(NUM_FACETS_PER_KEY_PROP_NAME)) {
      return numFacetsPerKey;
    }
    return Integer.valueOf(browseSelection.getSelectionProperties().get(NUM_FACETS_PER_KEY_PROP_NAME).toString());
  }
  @Override
  public RandomAccessFilter buildFilter(final BrowseSelection browseSelection) throws IOException {    
    final String[] values = browseSelection.getValues();
    final String[] notValues = browseSelection.getNotValues();
    final ValueOperation operation = browseSelection.getSelectionOperation();   
    
    if (values.length ==0 && notValues.length == 0) {
      return EmptyFilter.getInstance();
    } else if (values.length ==0 && notValues.length  > 0) {
      return new RandomAccessNotFilter(new PredicateFacetFilter(new SimpleDataCacheBuilder(getName()), new RangePredicate(notValues, values, operation, getSeparator(browseSelection))));
    } else  return new PredicateFacetFilter(new SimpleDataCacheBuilder(getName()), new RangePredicate(values, notValues, operation, getSeparator(browseSelection)));
  } 
  
  @Override
  public FacetCountCollectorSource getFacetCountCollectorSource(final BrowseSelection browseSelection, final FacetSpec ospec){
  return new FacetCountCollectorSource(){

    @Override
    public FacetCountCollector getFacetCountCollector(
        BoboIndexReader reader, int docBase) {
      MultiValueFacetDataCache dataCache = (MultiValueFacetDataCache) reader.getFacetData(_name);
      return new AttributesFacetCountCollector(AttributesFacetHandler.this, _name,dataCache,docBase,browseSelection, ospec, getFacetsPerKey(browseSelection), getSeparator(browseSelection));
    }
  };
  }
}
