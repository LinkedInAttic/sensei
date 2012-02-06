package com.senseidb.search.query.filters;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.filter.FacetFilter;
import com.browseengine.bobo.facets.filter.FacetFilter.FacetDocIdSetIterator;
import com.browseengine.bobo.util.BigNestedIntArray;

public class NullFilterConstructor extends FilterConstructor {
  public static final String FILTER_TYPE = "isNull";
  @Override
  protected Filter doConstructFilter(Object json) throws Exception {
    final String fieldName =  json instanceof String ? (String) json : ((JSONObject) json).getString("field");
    return new Filter() {
      
      @Override
      public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        final Object data = ((BoboIndexReader)reader).getFacetData(fieldName);
        if (data instanceof MultiValueFacetDataCache) {
          return new DocIdSet() {            
            @Override
            public DocIdSetIterator iterator() throws IOException {              
              return new MultiValueFacetDocIdSetIterator((MultiValueFacetDataCache)data, 0);
            }
          };
        } else if (data instanceof FacetDataCache) {
          return new DocIdSet() {            
            @Override
            public DocIdSetIterator iterator() throws IOException {              
              return new FacetFilter.FacetDocIdSetIterator((FacetDataCache) data, 0);
            }
          };
        }
        throw new UnsupportedOperationException("The null filter is supported only for the bobo facetHandlers that use FacetDataCache");
      }
    };
  }
  public final static class MultiValueFacetDocIdSetIterator extends FacetDocIdSetIterator
  {
      private final BigNestedIntArray _nestedArray;

      public MultiValueFacetDocIdSetIterator(MultiValueFacetDataCache dataCache, int index) 
      {
          super(dataCache, index);
          _nestedArray = dataCache._nestedArray;
      }
      
      @Override
      final public int nextDoc() throws IOException
      {
        return (_doc = (_doc < _maxID ? _nestedArray.findValue(_index, (_doc + 1), _maxID, true) : NO_MORE_DOCS));
      }

      @Override
      final public int advance(int id) throws IOException
      {
        if(_doc < id)
        {
          return (_doc = (id <= _maxID ? _nestedArray.findValue(_index, id, _maxID, true) : NO_MORE_DOCS));
        }
        return nextDoc();
      }
  }
}
