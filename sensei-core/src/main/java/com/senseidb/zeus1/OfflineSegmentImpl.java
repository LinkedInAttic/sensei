package com.senseidb.zeus1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.DocIdSet;

import com.browseengine.bobo.facets.data.TermValueList;

public class OfflineSegmentImpl implements OfflineSegment {
  Map<String, ForwardIndex> forwardIndexes = new HashMap<String, ForwardIndex>();
  Map<String, TermValueList> dictionaries = new HashMap<String, TermValueList>();
  int length;
  @Override
  public Map<String, Class<?>> getColumnTypes() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TermValueList<?> getDictionary(String column) {    
    return dictionaries.get(column);
  }

  @Override
  public DocIdSet[] getInvertedIndex(String column) {
    return null;
  }

  @Override
  public ForwardIndex getForwardIndex(String column) {
    return forwardIndexes.get(column);
  }

  @Override
  public int getLength() {
    return length;
  }

}
