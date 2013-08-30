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

package com.senseidb.facet.docset;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;


public final class EmptyDocIdSet extends DocIdSet
{
  private static EmptyDocIdSet SINGLETON=new EmptyDocIdSet();

  private static class EmptyDocIdSetIterator extends DocIdSetIterator
  {
    @Override
    public int docID() {	return -1; }

    @Override
    public int nextDoc() throws IOException { return DocIdSetIterator.NO_MORE_DOCS;  }

    @Override
    public int advance(int target) throws IOException {return DocIdSetIterator.NO_MORE_DOCS; }

    @Override
    public long cost() { return 0; }
  }

  private static EmptyDocIdSetIterator SINGLETON_ITERATOR = new EmptyDocIdSetIterator();

  private EmptyDocIdSet() { }

  public static EmptyDocIdSet getInstance()
  {
    return SINGLETON;
  }

  @Override
  public DocIdSetIterator iterator() 
  {
    return SINGLETON_ITERATOR;
  }

}
