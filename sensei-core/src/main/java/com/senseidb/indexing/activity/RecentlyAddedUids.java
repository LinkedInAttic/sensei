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
      elems.removeFirstLong();
    }
    elems.addAndMoveToLast(uid);
  }
  public synchronized int markRecentAsFoundInBitSet(long[] uids, BitSet found) {
    if (found.length() == 0) {
      return 0;
    }
    int ret = 0;
    int index = 0;
    while (true) {
      if (index < 0 || index >= found.length()) {
        break;
      }  
      index = found.nextClearBit(index);
      if (index < 0 || index >= found.length()) {
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
 public static void main(String[] args) {
   LongLinkedOpenHashSet elems = new LongLinkedOpenHashSet(100);
   for (int i = 0; i < 1000; i++) {
     elems.addAndMoveToLast(i % 20);
   }
   System.out.println(elems);
}
}
