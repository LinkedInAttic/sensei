package com.senseidb.indexing.activity.facet;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;

/**
 * Performs range iteration over activity fields
 * @author vzhabiuk
 *
 */
public class ActivityRangeLongFilterIterator extends DocIdSetIterator {
  private int _doc;
  protected final long[] fieldValues;
  private final long start;
  private final long end;
  private final int arrLength;
  private int[] indexes;

  public ActivityRangeLongFilterIterator(long[] fieldValues, int[] indexes,
          long start, long end) {
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
     long value = fieldValues[indexes[_doc]];      
     if (value >= start && value < end && value != Long.MIN_VALUE) {
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
