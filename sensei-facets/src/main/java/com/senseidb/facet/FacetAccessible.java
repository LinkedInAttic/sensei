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

import java.util.List;


public interface FacetAccessible {
  /**
   * Gets gathered top facets
   *
   * @return list of facets
   */
  List<Facet> getTopFacets();

  /**
   * Gets the facet given a value. This is a way for random accessing
   * into the facet data structure.
   *
   * @param value Facet value
   * @return a facet with count filled in
   */
  Facet getFacet(String value);

  /**
   * Responsible for release resources used. If the implementing class
   * does not use a lot of resources,
   * it does not have to do anything.
   */
  public void close();

  /**
   * Returns an iterator to visit all the facets
   *
   * @return Returns a FacetIterator to iterate over all the facets
   */
  FacetIterator iterator();
}
