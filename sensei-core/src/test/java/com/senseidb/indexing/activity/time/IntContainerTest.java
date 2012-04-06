package com.senseidb.indexing.activity.time;

import static org.junit.Assert.assertEquals;
import junit.framework.TestCase;

import org.junit.Test;

public class IntContainerTest extends TestCase {

 
  public void test1() {
    IntContainer intContainer = new IntContainer();
    assertEquals(0, intContainer.getSize());
    for (int i = 0; i < 100; i++) {
      intContainer.add(i);
    }
    assertEquals(100, intContainer.getSize());
    assertEquals(0, intContainer.peekFirst());
    assertEquals(0, intContainer.removeFirst());
    assertEquals(1, intContainer.peekFirst());
    assertEquals(99, intContainer.removeLast());
    assertEquals(98, intContainer.getSize());
    intContainer.add(99);
    assertEquals(99, intContainer.getSize());
    for (int i = 0; i < 90; i++) {
      if (i % 2 == 1) {
        intContainer.removeFirst();
      } else {
        intContainer.removeLast();
      }
    }
    for (int i = 0; i < 10; i++) {
      intContainer.add(i);
    }
    assertEquals(19, intContainer.getSize());
    assertEquals(0, intContainer.startIndex);
    assertEquals(21, intContainer.array.length);
    for (int i = 0; i < 18; i++) {
      intContainer.removeFirst();
    }
    assertEquals(1, intContainer.getSize());
    assertEquals(3, intContainer.startIndex);
    assertEquals(5, intContainer.array.length);
  }

}
