package com.senseidb.ba;

import com.browseengine.bobo.facets.data.TermValueList;

/**
 * The forward index, that allows to get the dictionary value by docId in constant time. Without compression it would be just the int array
 * For Pinot we may use the fixed length encoding, so that each element would require Math.ceil(log2(TermValueList.getLength)) bits.
 * Might be stored off heap
 * There is a chance that we may reuse trevni in memory ColumnValues https://github.com/vzhabiuk/trevni/blob/master/java/core/src/main/java/org/apache/trevni/ColumnValues.java,
 * although it would slow down the getValueIndex call, which is the main source of latency during the search
 * 
 *
 */
public interface ForwardIndex {
  int getLength();
  int getValueIndex(int docId);
  /**
   * Used for the facet values, if there are no filtering
   */
  int getFrequency(int valueId);
  TermValueList<?> getDictionary();
}