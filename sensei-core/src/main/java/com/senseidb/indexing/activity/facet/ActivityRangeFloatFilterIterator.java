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
package com.senseidb.indexing.activity.facet;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;

/**
 * Performs range iteration over activity fields
 * @author vzhabiuk
 *
 */
public class ActivityRangeFloatFilterIterator extends DocIdSetIterator {
  private int _doc;
  protected final float[] fieldValues;
  private final int start;
  private final int end;
  private final int arrLength;
  private int[] indexes;

  public ActivityRangeFloatFilterIterator(float[] fieldValues, int[] indexes,
      int start, int end) {
    this.fieldValues = fieldValues;
    this.start = start;
    this.end = end;
    this.indexes = indexes;
    arrLength = indexes.length;
    _doc = -1;
  }
  @Override
  final public int docID() {
    return _doc;
  }
  @Override
  public int nextDoc() throws IOException {  
   while (++_doc < arrLength ) {
     if (indexes[_doc] == -1) {
       continue;
     }
     float value = fieldValues[indexes[_doc]];      
     if (value >= start && value < end && value != Float.MIN_VALUE) {
       return _doc;
     }
   }
   return NO_MORE_DOCS;
  }

  @Override
  public int advance(int id) throws IOException {
    _doc = id - 1;
    return nextDoc();
  }
}
