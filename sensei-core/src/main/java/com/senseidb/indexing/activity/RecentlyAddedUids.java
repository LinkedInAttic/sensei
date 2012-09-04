package com.senseidb.indexing.activity;

import java.util.BitSet;

import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet;

public class RecentlyAddedUids {
  private final int capacity;  
  private LongLinkedOpenHashSet elems;
  public RecentlyAddedUids(int capacity) {
    this.capacity = capacity;
    elems = new LongLinkedOpenHashSet(capacity);
  }
  
  public synchronized void add(long uid) {
    if (elems.size() == capacity) {
      elems.remove(elems.firstLong());
    }
    elems.add(uid);
  }
  public synchronized int markRecentAsFoundInBitSet(long[] uids, BitSet found, int bitSetLength) {
    if (found.length() == 0) {
      return 0;
    }
    int ret = 0;
    int index = 0;
    while (true) {
      if (index < 0 || index >= bitSetLength) {
        break;
      }  
      index = found.nextClearBit(index);
      if (index < 0 || index >= bitSetLength) {
        break;
      }     
      
      if (elems.contains(uids[index])) {
       
        found.set(index);
        ret++;
      } else {
      }
      index++;
    }
    return ret;
  }
  protected synchronized void clear() {
    elems.clear();
  }
 
}
