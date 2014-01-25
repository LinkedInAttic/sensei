/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
package com.senseidb.search.query.filters;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.json.JSONObject;
import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.filter.FacetFilter;
import com.browseengine.bobo.facets.filter.FacetFilter.FacetDocIdSetIterator;
import com.browseengine.bobo.util.BigNestedIntArray;

public class NullFilterConstructor extends FilterConstructor {
  public static final String FILTER_TYPE = "isNull";
  private static final Logger log = Logger.getLogger(NullFilterConstructor.class);

  @Override
  protected SenseiFilter doConstructFilter(Object json) throws Exception {
    final String fieldName =  json instanceof String ? (String) json : ((JSONObject) json).getString("field");
    return new SenseiFilter() {
      @Override
      public SenseiDocIdSet getSenseiDocIdSet(IndexReader reader) throws IOException {
        BoboIndexReader boboReader = (BoboIndexReader) reader;
        FacetHandler facetHandler = boboReader.getFacetHandler(fieldName);
        Object facetData = facetHandler.getFacetData(boboReader);

        String plan = EMPTY_STRING;

        if(facetData instanceof MultiValueFacetDataCache)
        {
          final MultiValueFacetDataCache facetDataCache = (MultiValueFacetDataCache) facetData;

          DocIdSet docIdSet = new DocIdSet() {
            @Override
            public DocIdSetIterator iterator() throws IOException {
              return new MultiValueFacetDocIdSetIterator(facetDataCache, 0);
            }
          };

          if(log.isDebugEnabled()) {
            plan = fieldName + " IS MULTIVALUE NULL";
          }

          return new SenseiDocIdSet(docIdSet, DocIdSetCardinality.exact(facetDataCache.freqs[0], boboReader.maxDoc() + 1), plan);
        }
        else if (facetData instanceof FacetDataCache)
        {
          final FacetDataCache facetDataCache = (FacetDataCache) facetData;

          DocIdSet docIdSet = new DocIdSet() {
            @Override
            public DocIdSetIterator iterator() throws IOException {
              return new FacetFilter.FacetDocIdSetIterator(facetDataCache, 0);
            }
          };

          if(log.isDebugEnabled()) {
            plan = fieldName + " IS NULL";
          }
          return new SenseiDocIdSet(docIdSet, DocIdSetCardinality.exact(facetDataCache.freqs[0], boboReader.maxDoc() + 1), plan);
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
