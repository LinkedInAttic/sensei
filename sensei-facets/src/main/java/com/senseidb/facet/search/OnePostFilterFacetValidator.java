package com.senseidb.facet.search;


import com.senseidb.facet.handler.FacetCountCollector;

import java.io.IOException;


/**
 * @author Dmytro Ivchenko
 */
public class OnePostFilterFacetValidator extends FacetValidator
{
  private FacetContext _firsttime;

  public OnePostFilterFacetValidator(FacetContext[] collectors)
      throws IOException
  {
    super(collectors, 1);
    _firsttime = _facetContexts[0];
  }

  @Override
  public final boolean validate(int docid)
      throws IOException
  {
    int facetDocId = _firsttime.getFacetHitIterator().docID();
    if (facetDocId < docid)
      facetDocId = _firsttime.getFacetHitIterator().advance(facetDocId);

    if (facetDocId != docid)
    {
      _nextTarget = facetDocId;
      _firsttime.getCountCollector().collect(docid);
      return false;
    }
    else
    {
      _nextTarget = facetDocId + 1;
      for (FacetCountCollector collector : _countCollectors)
      {
        collector.collect(docid);
      }
      return true;
    }
  }
}