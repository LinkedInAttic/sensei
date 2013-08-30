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

package com.senseidb.facet;

import java.util.Iterator;

/**
 * Iterator to iterate over facets
 *
 * @author nnarkhed
 */
public abstract class FacetIterator implements Iterator<Comparable> {

  public int count;
  public Comparable facet;

  /**
   * Moves the iteration to the next facet
   *
   * @return the next facet value
   */
  public abstract Comparable next();

  /**
   * Moves the iteration to the next facet whose hitcount >= minHits. returns null if there is no facet whose hitcount >= minHits.
   * Hence while using this method, it is useless to use hasNext() with it.
   * After the next() method returns null, calling it repeatedly would result in undefined behavior
   *
   * @return The next facet value. It returns null if there is no facet whose hitcount >= minHits.
   */
  public abstract Comparable next(int minHits);

  public abstract String format(Object val);
}
