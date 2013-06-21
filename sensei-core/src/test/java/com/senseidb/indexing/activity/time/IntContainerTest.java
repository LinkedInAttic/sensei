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
