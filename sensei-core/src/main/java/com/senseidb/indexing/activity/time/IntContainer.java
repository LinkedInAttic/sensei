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
package com.senseidb.indexing.activity.time;

import java.util.Arrays;



/**
 * A copy on write int array wrapper, optimized to store activity updates
 * @author vzhabiuk
 *
 */
public class IntContainer {
  private static int[] EMPTY_ARR = new int[0];
  private static final int initialGrowthFactor = 2;
  private static final int capacityThreshold = 10;
  
  protected int[] array;
  protected int startIndex = 0;
  protected int actualSize = 0;


  public IntContainer(int capacity) {
    if (capacity == 0) {
      array = EMPTY_ARR;
    } else {
      array = new int[capacity];
    }
  }

  public IntContainer() {
    array = new int[1];
  }

  public int removeFirst() {    
    ensureCapacityOnStart();
    if (actualSize == 0) {
      throw new IllegalStateException("The collection is empty");
    }
    startIndex++;
    actualSize--;
    return array[startIndex - 1];
  }

  public int removeLast() {
    ensureCapacityOnStart();
    if (actualSize == 0) {
      throw new IllegalStateException("The collection is empty");
    }
    actualSize--;
    return array[startIndex + actualSize];
  }
  public int getSize() {
    return actualSize;
  }
  public int peekFirst() {
    return array[startIndex];
  }
  public int peekLast() {
    return array[startIndex + actualSize - 1];
  }
  public int get(int index) {
    return array[startIndex + index];
  }
  public IntContainer add(int number) {
    ensureCapacityOnEnd();
    array[startIndex + actualSize] = number;
    actualSize++;
    return this;
  }

  private void ensureCapacityOnEnd() {    
    if (actualSize + startIndex < array.length) {
      return;
    }
    double growthFactor = 1.2;
    
    int newSize = array.length < capacityThreshold ? array.length + initialGrowthFactor : (int) (array.length * growthFactor);
    int[] oldArr = array;
    array = new int[newSize];
    System.arraycopy(oldArr, startIndex, array, 0, actualSize);
    startIndex = 0;
  }

  private void ensureCapacityOnStart() {
      int newStartIndex =  startIndex;
      int newArrayLength = array.length;
      int reduceFactor = 2;
      if (actualSize >= capacityThreshold && startIndex > actualSize / (reduceFactor * reduceFactor)) {
        newStartIndex = 0;
      } else if (startIndex > reduceFactor && actualSize < capacityThreshold) {
        newStartIndex = 0;
      }
     if (array.length > reduceFactor && actualSize < array.length / reduceFactor) {
       newArrayLength = array.length / reduceFactor;
     } 
     if (newStartIndex != startIndex || newArrayLength != array.length) {
       int[] oldArr = array;
       if (newArrayLength != array.length) {
         array = new int[newArrayLength];         
       }
       System.arraycopy(oldArr, startIndex, array, 0, actualSize);
       startIndex = 0;
     }
  }
  @Override
  public String toString() {
    return Arrays.toString(array);
  }
  public int size() {
    return actualSize;
  }
}
