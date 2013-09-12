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

package com.senseidb.facet.handler;


import com.senseidb.facet.FacetSelection;
import com.senseidb.facet.search.FacetAtomicReader;
import com.senseidb.facet.FacetSpec;
import com.senseidb.facet.filter.AndFilter;
import com.senseidb.facet.filter.EmptyFilter;
import com.senseidb.facet.filter.NotFilter;
import com.senseidb.facet.filter.OrFilter;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Filter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * FacetHandler definition
 */
public abstract class FacetHandler {

  protected final String _name;
  private final Set<String> _dependsOn;
  private final Map<String, FacetHandler> _dependedFacetHandlers;

  /**
   * Constructor
   *
   * @param name      name
   * @param dependsOn Set of names of _facet handlers this _facet handler depend on for loading
   */
  public FacetHandler(String name, Set<String> dependsOn) {
    _name = name;
    _dependsOn = new HashSet<String>();
    if (dependsOn != null) {
      _dependsOn.addAll(dependsOn);
    }
    _dependedFacetHandlers = new HashMap<String, FacetHandler>();
  }

  /**
   * Constructor
   *
   * @param name name
   */
  public FacetHandler(String name) {
    this(name, null);
  }

  /**
   * Gets the name
   *
   * @return name
   */
  public final String getName() {
    return _name;
  }

  /**
   * Gets names of the _facet handler this depends on
   *
   * @return set of _facet handler names
   */
  public final Set<String> getDependsOn() {
    return _dependsOn;
  }

  /**
   * Adds a list of depended _facet handlers
   *
   * @param facetHandler depended _facet handler
   */
  public final void putDependedFacetHandler(FacetHandler facetHandler) {
    _dependedFacetHandlers.put(facetHandler._name, facetHandler);
  }

  /**
   * Gets a depended _facet handler
   *
   * @param name _facet handler name
   * @return _facet handler instance
   */
  public final FacetHandler getDependedFacetHandler(String name) {
    return _dependedFacetHandlers.get(name);
  }

  /**
   * Gets a filter from a given selection
   *
   * @param sel selection
   * @return a filter
   * @throws java.io.IOException
   * @throws java.io.IOException
   */
  public Filter buildFilter(FacetSelection sel) throws IOException {
    List<String> selections = sel.getValues();
    List<String> notSelections = sel.getNotValues();

    Filter filter = null;
    if (selections != null && selections.size() > 0) {
      if (sel.getSelectionOperation() == FacetSelection.ValueOperation.ValueOperationAnd) {
        filter = buildAndFilter(selections);
        if (filter == null) {
          filter = EmptyFilter.getInstance();
        }
      } else {
        filter = buildOrFilter(selections, false);
        if (filter == null) {
          return EmptyFilter.getInstance();
        }
      }
    }

    if (notSelections != null && notSelections.size() > 0) {
      Filter notFilter = buildOrFilter(notSelections, true);
      if (filter == null) {
        filter = notFilter;
      } else {
        Filter andFilter = new AndFilter(Arrays.asList(new Filter[]{filter, notFilter}));
        filter = andFilter;
      }
    }

    return filter;
  }

  abstract public Filter buildFilter(String value) throws IOException;

  public Filter buildAndFilter(List<String> vals) throws IOException {
    ArrayList<Filter> filterList = new ArrayList<Filter>(vals.size());

    for (String val : vals) {
      Filter f = buildFilter(val);
      if (f != null) {
        filterList.add(f);
      } else {
        return EmptyFilter.getInstance();
      }
    }

    if (filterList.size() == 1)
      return filterList.get(0);
    return new AndFilter(filterList);
  }

  public Filter buildOrFilter(List<String> vals, boolean isNot) throws IOException {
    ArrayList<Filter> filterList = new ArrayList<Filter>(vals.size());

    for (String val : vals) {
      Filter f = buildFilter(val);
      if (f != null && !(f instanceof EmptyFilter)) {
        filterList.add(f);
      }
    }

    Filter finalFilter;
    if (filterList.size() == 0) {
      finalFilter = EmptyFilter.getInstance();
    } else {
      finalFilter = new OrFilter(filterList);
    }

    if (isNot) {
      finalFilter = new NotFilter(finalFilter);
    }
    return finalFilter;
  }

  /**
   * Gets a FacetCountCollector
   *
   * @param sel   selection
   * @param fspec facetSpec
   * @return a FacetCountCollector
   */
  abstract public FacetCountCollectorSource getFacetCountCollectorSource(FacetSelection sel, FacetSpec fspec);

  /**
   * Gets the field value
   *
   * @param id     doc
   * @param reader index reader
   * @return array of field values
   */
  abstract public String[] getFieldValues(FacetAtomicReader reader, int id);

  public int getNumItems(FacetAtomicReader reader, int id) {
    throw new UnsupportedOperationException("getNumItems is not supported for this _facet handler: " + getClass().getName());
  }

  public Object[] getRawFieldValues(FacetAtomicReader reader, int id) {
    return getFieldValues(reader, id);
  }

  /**
   * Gets a single field value
   *
   * @param id     doc
   * @param reader index reader
   * @return first field value
   */
  public String getFieldValue(FacetAtomicReader reader, int id) {
    return getFieldValues(reader, id)[0];
  }

  /**
   * builds a comparator to determine how sorting is done
   *
   * @return a sort comparator
   */
  abstract public FieldComparatorSource getFieldComparatorSource();

  /**
   * Do some necessary cleanup work after load
   *
   * @param reader
   */
  public void cleanup(FacetAtomicReader reader) {
    // do nothing
  }
}
