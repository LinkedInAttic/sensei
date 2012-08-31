package com.senseidb.ba;

import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermStringList;
import com.senseidb.indexing.DefaultSenseiInterpreter;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntBidirectionalIterator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import it.unimi.dsi.fastutil.longs.LongBidirectionalIterator;
import it.unimi.dsi.fastutil.longs.LongList;

public class DictionaryCreator {
  private IntAVLTreeSet intAVLTreeSet;
  private LongAVLTreeSet longAVLTreeSet;
  private TreeSet<String> stringSet;
  private Int2IntOpenHashMap int2IntMap;
  public DictionaryCreator() {
    intAVLTreeSet = new IntAVLTreeSet();
    
  }
  public void addIntValue(int value) {
    intAVLTreeSet.add(value); 
  }
  public void addLongValue(long value) {
    longAVLTreeSet.add(value); 
  }
  public void addStringValue(String value) {
    stringSet.add(value); 
  }
  public TermIntList produceIntDictionary() {  
    TermIntList termIntList = new TermIntList(intAVLTreeSet.size(), DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(int.class));
    IntBidirectionalIterator iterator = intAVLTreeSet.iterator();
    termIntList.add(null);
    while (iterator.hasNext()) {
      ((IntList)termIntList.getInnerList()).add(iterator.nextInt());
    }    
    termIntList.seal();
    int[] elements = termIntList.getElements();
    int2IntMap = new Int2IntOpenHashMap(intAVLTreeSet.size());
    for (int i = 1; i< elements.length; i++) {
      int2IntMap.put(elements[i], i);
    }
    return termIntList;
  }
  public Int2IntOpenHashMap getIndexIntMap() {      
    return int2IntMap;
  }
  public TermLongList produceLongDictionary() {  
    TermLongList termlongList = new TermLongList(longAVLTreeSet.size(), DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(long.class));
    LongBidirectionalIterator iterator = longAVLTreeSet.iterator();
    termlongList.add(null);
    while (iterator.hasNext()) {
      ((LongList)termlongList.getInnerList()).add(iterator.nextLong());
    }    
    termlongList.seal();
    return termlongList;
  }
  public TermStringList produceStringDictionary() {  
    TermStringList termStringList = new TermStringList(stringSet.size());
    Iterator<String> iterator = stringSet.iterator();
    termStringList.add(null);
    while (iterator.hasNext()) {
      ((List<String>)termStringList.getInnerList()).add(iterator.next());
    }
    termStringList.seal();
    return termStringList;
  }
}
