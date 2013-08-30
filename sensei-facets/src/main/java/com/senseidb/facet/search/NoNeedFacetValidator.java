package com.senseidb.facet.search;


import com.senseidb.facet.handler.FacetCountCollector;

import java.io.IOException;


/**
 * @author Dmytro Ivchenko
 */
public class NoNeedFacetValidator extends FacetValidator
{
  public NoNeedFacetValidator(FacetContext[] collectors)
      throws IOException
  {
    super(collectors, 0);
  }

  @Override
  public final boolean validate(int docid)
      throws IOException
  {
    for (FacetCountCollector collector : _countCollectors)
    {
      collector.collect(docid);
    }
    return true;
  }
}
