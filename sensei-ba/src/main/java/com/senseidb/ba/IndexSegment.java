package com.senseidb.ba;

import java.util.Map;

import org.apache.lucene.search.DocIdSet;

import com.browseengine.bobo.facets.data.TermValueList;


/**
 * Is used instead of the ZoieIndexReader in Sensei. Will be constructed from the trevni index files and be kept in memory
 *
 */
public interface IndexSegment {
   Map<String, Class<?>> getColumnTypes();
   /**
   * get the sorted array of unique column values
   */
  TermValueList<?> getDictionary(String column);
   /**
   * Only dimension columns need inverted index. We might use kamikaze's implementation of the p4delta compression for the inverted index http://sna-projects.com/kamikaze/quickstart.php
   * If the column's cardinality is << 32 we might use compressed bitset index instead of p4delta 
   * We maintain the inverted index per each column value. 
   */
   DocIdSet[] getInvertedIndex(String column);
   ForwardIndex  getForwardIndex(String column);
   /**
   * number of docs in the index
   */
  int getLength();
   
}
