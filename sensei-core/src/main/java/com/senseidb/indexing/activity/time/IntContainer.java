package com.senseidb.indexing.activity.time;

import scala.actors.threadpool.Arrays;

public class IntContainer {
  private static int[] EMPTY_ARR = new int[0];
  
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
    System.out.println("ensureCapacityOnEnd = " + array.length);
    int newSize = array.length < 10 ? 10 : array.length * 2;
    int[] oldArr = array;
    array = new int[newSize];
    System.arraycopy(oldArr, startIndex, array, 0, actualSize);
    startIndex = 0;
  }

  private void ensureCapacityOnStart() {
      int newStartIndex =  startIndex;
      int newArrayLength = array.length;
      if (actualSize > 9 && startIndex > actualSize / 4) {
        newStartIndex = 0;
      } else if (actualSize <= 9 && startIndex > 2) {
        newStartIndex = 0;
      }
     if (array.length > 2 && actualSize < array.length / 2) {
       newArrayLength = array.length / 2;
     } 
     if (newStartIndex != startIndex || newArrayLength != array.length) {
       int[] oldArr = array;
       if (newArrayLength != array.length) {
         array = new int[newArrayLength];
         System.out.println("ensureCapacityOnStart");
       }
       System.arraycopy(oldArr, startIndex, array, 0, actualSize);
       startIndex = 0;
     }
  }
  @Override
  public String toString() {
    // TODO Auto-generated method stub
    return Arrays.toString(array);
  }
  public int size() {
    return actualSize;
  }
}
