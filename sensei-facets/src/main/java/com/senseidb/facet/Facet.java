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

import java.util.Comparator;
import java.util.List;


/**
 * This class represents a facet
 */
public class Facet {

  private String _value;
  private int _hitcount;

  public Facet() {
  }

  public Facet(String value, int hitcount) {
    _value = value;
    _hitcount = hitcount;
  }

  /**
   * Gets the facet value
   *
   * @return value
   * @see #setValue(String)
   */
  public String getValue() {
    return _value;
  }

  /**
   * Sets the facet value
   *
   * @param value Facet value
   * @see #getValue()
   */
  public Facet setValue(String value) {
    _value = value;
    return this;
  }

  /**
   * Gets the hit count
   *
   * @return hit count
   */
  public int getFacetValueHitCount() {
    return _hitcount;
  }

  /**
   * Sets the hit count
   *
   * @param hitcount Hit count
   */
  public Facet setFacetValueHitCount(int hitcount) {
    _hitcount = hitcount;
    return this;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(_value).append("(").append(_hitcount).append(")");
    return buf.toString();
  }


  @Override
  public boolean equals(Object obj) {
    boolean equals = false;

    if (obj instanceof Facet) {
      Facet c2 = (Facet) obj;
      if (_hitcount == c2._hitcount && _value.equals(c2._value)) {
        equals = true;
      }
    }
    return equals;
  }

  public List<Facet> merge(List<Facet> v, Comparator<Facet> comparator) {
    int i = 0;
    for (Facet facet : v) {
      int val = comparator.compare(this, facet);
      if (val == 0) {
        facet._hitcount += this._hitcount;
        return v;
      }
      if (val > 0) {

      }
      i++;
    }
    v.add(this);
    return v;
  }
}
