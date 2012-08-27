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
