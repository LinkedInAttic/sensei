package com.senseidb.ba;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.indexing.DefaultSenseiInterpreter;
import com.senseidb.util.SenseiDefaults;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntBidirectionalIterator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import it.unimi.dsi.fastutil.longs.LongBidirectionalIterator;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

public class ForwardIndexBackedByArray implements ForwardIndex {
  private final String column;
  private TermValueList<?> dictionary;
  private int[] forwardIndex;
  private int[] freqs;
  public ForwardIndexBackedByArray(String column) {
    this.column = column;
  }
  public void initByLongValues(List<Long> values) {
    LongAVLTreeSet longAVLTreeSet = new LongAVLTreeSet();
    for (Long value : values) {
      if (value != null) {
        longAVLTreeSet.add(value);
      }
    }
    TermLongList termLongList = new TermLongList(longAVLTreeSet.size(), DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(long.class));
    termLongList.add(null);
    LongBidirectionalIterator iterator = longAVLTreeSet.iterator();
    while (iterator.hasNext()) {
      long nextLong = iterator.nextLong();
      ((LongList)termLongList.getInnerList()).add(nextLong);
    }
    dictionary = termLongList;
    dictionary.seal();
    long[] elements = termLongList.getElements();
    Long2IntMap long2IntMap = new Long2IntOpenHashMap(values.size());
    for (int i = 1; i< elements.length; i++) {
      long2IntMap.put(elements[i], i);
    }
    forwardIndex = new int[values.size()];
    freqs = new int[termLongList.size()];
    for (int i = 0; i< values.size(); i++) {
      int dictIndex = values.get(i) != null && long2IntMap.containsKey(values.get(i)) ? long2IntMap.get(values.get(i)) : 0;
      forwardIndex[i] = dictIndex;
      freqs[dictIndex]++;
    }
  }
 
  public void initByIntValues(List<Integer> values) {
    IntAVLTreeSet intAVLTreeSet = new IntAVLTreeSet();
    for (Integer value : values) {
      if (value != null) {
        intAVLTreeSet.add(value);
      }
    }
    TermIntList termIntList = new TermIntList(intAVLTreeSet.size(), DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(int.class));
    IntBidirectionalIterator iterator = intAVLTreeSet.iterator();
    termIntList.add(null);
    while (iterator.hasNext()) {
      ((IntList)termIntList.getInnerList()).add(iterator.nextInt());
    }
    dictionary = termIntList;
    dictionary.seal();
    int[] elements = termIntList.getElements();
    Int2IntMap int2IntMap = new Int2IntOpenHashMap(values.size());
    for (int i = 1; i< elements.length; i++) {
      int2IntMap.put(elements[i], i);
    }
    forwardIndex = new int[values.size()];
    freqs = new int[termIntList.size()];
    for (int i = 0; i< values.size(); i++) {
      int dictIndex = values.get(i) != null && int2IntMap.containsKey(values.get(i)) ? int2IntMap.get(values.get(i)) : 0;
      forwardIndex[i] = dictIndex;
      freqs[dictIndex]++;
    }
  }
 
  public void initByStringValues(List<String> values) {
    SortedSet<String> treeSet = new TreeSet<String>();
    for (String value :values ) {
      if (value != null) {
        treeSet.add(value);
      }
    }
    TermStringList termStringList = new TermStringList(treeSet.size());
    termStringList.add(null);
    Iterator<String> iterator = treeSet.iterator();
    
    while (iterator.hasNext()) {
      ((List<String>)termStringList.getInnerList()).add(iterator.next());
    }
    dictionary = termStringList;
    dictionary.seal();
    Object2IntMap<String> obj2IntMap = new Object2IntOpenHashMap<String>(values.size());
    for (int i = 1; i< termStringList.size(); i++) {
      obj2IntMap.put(termStringList.get(i), i);
    }
    forwardIndex = new int[values.size()];
    freqs = new int[termStringList.size()];
    for (int i = 0; i< values.size(); i++) {
      int dictIndex = values.get(i) != null && obj2IntMap.containsKey(values.get(i)) ? obj2IntMap.get(values.get(i)) : 0;
      forwardIndex[i] = dictIndex;
      freqs[dictIndex]++;
    }
  }
  
  
  
  @Override
  public int getLength() {
    return forwardIndex.length;
  }

  @Override
  public int getValueIndex(int docId) {
    return forwardIndex[docId];
  }

  @Override
  public int getFrequency(int valueId) {
    return freqs[valueId];
  }
  public TermValueList<?> getDictionary() {
    return dictionary;
  }
  
}
