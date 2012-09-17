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
