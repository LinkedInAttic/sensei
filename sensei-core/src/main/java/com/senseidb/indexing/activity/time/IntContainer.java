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
