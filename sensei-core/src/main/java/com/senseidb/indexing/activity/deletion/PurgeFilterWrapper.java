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
package com.senseidb.indexing.activity.deletion;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;

import proj.zoie.api.ZoieIndexReader;

/**
 * This is used to notify the activity engine if the document gets deleted from Zoie by executing the purge filter
 * @author vzhabiuk
 *
 */
public class PurgeFilterWrapper extends Filter {  
  private final Filter internal;
  private final DeletionListener deletionListener;

  public PurgeFilterWrapper(Filter internal, DeletionListener deletionListener) {
    this.internal = internal;
    this.deletionListener = deletionListener;   
  }
  
  @Override
  public DocIdSet getDocIdSet(final IndexReader reader) throws IOException {    
    final ZoieIndexReader zoieIndexReader = (ZoieIndexReader)reader; 
    return new DocIdSet() {
      public DocIdSetIterator iterator() throws IOException {
        return new DocIdSetIteratorWrapper(internal.getDocIdSet(reader).iterator()) {          
          @Override
          protected void handeDoc(int ret) {            
            deletionListener.onDelete(reader, zoieIndexReader.getUID(ret));            
          }
        };
      }
    };
  }

  public abstract static class DocIdSetIteratorWrapper extends DocIdSetIterator {
  private final DocIdSetIterator iterator;
  public DocIdSetIteratorWrapper(DocIdSetIterator iterator) {
    this.iterator = iterator;
  }
    @Override
    public int docID() {
      return iterator.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int ret = iterator.nextDoc();
      if (ret != DocIdSetIterator.NO_MORE_DOCS) {
        handeDoc(ret);
      }
      return ret;
    }

    @Override
    public int advance(int target) throws IOException {
      int ret = iterator.advance(target);
      if (ret != DocIdSetIterator.NO_MORE_DOCS) {
        handeDoc(ret);
      }
      return ret;
    }
    protected abstract void handeDoc(int ret);    
  }
  
}
