package com.senseidb.facet.search;


import com.senseidb.facet.handler.FacetCountCollector;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;


/**
 * @author Dmytro Ivchenko
 */
public class DefaultFacetValidator extends FacetValidator
{

  public DefaultFacetValidator(FacetContext[] collectors, int numPostFilters)
      throws IOException
  {
    super(collectors, numPostFilters);
  }

  /**
   * This method validates the doc against any multi-select enabled fields.
   *
   * @param docid
   *
   * @return true if all fields matched
   */
  @Override
  public final boolean validate(final int docid)
      throws IOException
  {
    FacetContext firstMiss = null;
    for (int i = 0; i < _numPostFilters; i++)
    {
      FacetContext facetContext = _facetContexts[i];
      int sid = facetContext.getFacetHitIterator().docID();

      if (sid < docid)
      {
        sid = facetContext.getFacetHitIterator().advance(docid);
        if (sid == DocIdSetIterator.NO_MORE_DOCS)
        {
          // move this to front so that the call can find the failure faster
          FacetContext tmp = _facetContexts[0];
          _facetContexts[0] = _facetContexts[i];
          _facetContexts[i] = tmp;
        }
      }

      if (sid > docid) //mismatch
      {
        if (firstMiss != null)
        {
          // failed because we already have a mismatch
          _nextTarget = Math.min(firstMiss.getFacetHitIterator().docID(), facetContext.getFacetHitIterator().docID());
          return false;
        }
        firstMiss = facetContext;
      }
    }

    _nextTarget = docid + 1;

    if (firstMiss != null)
    {
      firstMiss.getCountCollector().collect(docid);
      return false;
    }
    else
    {
      for (FacetCountCollector collector : _countCollectors)
      {
        collector.collect(docid);
      }
      return true;
    }
  }
}