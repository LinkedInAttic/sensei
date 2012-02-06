package com.senseidb.plugin;

import java.util.Map;

import com.browseengine.bobo.facets.data.FacetDataFetcher;
import com.browseengine.bobo.facets.data.TermListFactory;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.impl.VirtualSimpleFacetHandler;

public class VirtualSimpleFacetHandlerSenseiPluginFactory extends AbstractFacetHandlerSenseiPluginFactory
{
  public static final String TERMLISTFACTORY = "termListFactory";
  public static final String FACETDATAFETCHER = "facetDataFetcher";

  @Override
  public FacetHandler getBean(Map<String,String> initProperties,
                              String fullPrefix,
                              SenseiPluginRegistry pluginRegistry)
  {
    FacetDataFetcher facetDataFetcher = pluginRegistry.getBeanByName(initProperties.get(FACETDATAFETCHER),
                                                                     FacetDataFetcher.class);
    if (facetDataFetcher == null)
      throw new IllegalArgumentException(FACETDATAFETCHER + " is not set");

    TermListFactory termListFactory = TermListFactorySenseiPluginFactory.getFactory(
                                        initProperties.get(TERMLISTFACTORY));
    if (termListFactory == null)
    {
      termListFactory = pluginRegistry.getBeanByName(initProperties.get(TERMLISTFACTORY),
                                                     TermListFactory.class);
    }

    return new VirtualSimpleFacetHandler(SenseiPluginRegistry.getNameByPrefix(fullPrefix),
                                         termListFactory,
                                         facetDataFetcher,
                                         getDepends(initProperties));
  }
}
