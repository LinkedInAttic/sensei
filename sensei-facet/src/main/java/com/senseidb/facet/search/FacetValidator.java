package com.senseidb.facet.search;


import com.senseidb.facet.handler.FacetCountCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * @author Dmytro Ivchenko
 */
public abstract class FacetValidator
{
  protected final FacetContext[] _facetContexts;
  protected final List<FacetCountCollector> _countCollectors;
  protected final int _numPostFilters;
  public int _nextTarget;

  public FacetValidator(FacetContext[] facetContexts)
  {
    this(facetContexts, 0);
  }

  public FacetValidator(FacetContext[] facetContexts, int numPostFilters)
  {
    _numPostFilters = numPostFilters;
    _facetContexts = facetContexts;
    _countCollectors = new ArrayList<FacetCountCollector>(facetContexts.length);
    for (int i = 0; i < facetContexts.length; ++i)
    {
      FacetCountCollector countCollector = facetContexts[i].getCountCollector();
      if (countCollector != null)
        _countCollectors.add(countCollector);
    }
  }
  /**
   * This method validates the doc against any multi-select enabled fields.
   * @param docid
   * @return true if all fields matched
   */
  public abstract boolean validate(final int docid)
      throws IOException;
}