package com.senseidb.test.plugin;

import java.util.HashSet;
import java.util.Map;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.FacetDataFetcher;
import com.browseengine.bobo.facets.data.PredefinedTermListFactory;
import com.browseengine.bobo.facets.data.TermFixedLengthLongArrayListFactory;
import com.browseengine.bobo.facets.impl.VirtualSimpleFacetHandler;
import com.senseidb.plugin.SenseiPluginFactory;
import com.senseidb.plugin.SenseiPluginRegistry;

public class VirtualGroupIdFactory implements SenseiPluginFactory<VirtualSimpleFacetHandler> {
  @Override
  public VirtualSimpleFacetHandler getBean(Map<String, String> initProperties, String fullPrefix,
      SenseiPluginRegistry pluginRegistry) {
    //the decision also can be made by the full prefix
    if ("default".equals(initProperties.get("typeProp"))) {
      HashSet<String> depends = new HashSet<String>();
      depends.add("groupid");
      return new VirtualSimpleFacetHandler("virtual_groupid", new PredefinedTermListFactory(Long.class, "00000000000000000000000000000000000"), facetDataFetcher, depends);
    }
    if ("fixedlengthlongarray".equals(initProperties.get("typeProp"))) {
      HashSet<String> depends = new HashSet<String>();
      depends.add("groupid");
      return new VirtualSimpleFacetHandler("virtual_groupid_fixedlengthlongarray", new TermFixedLengthLongArrayListFactory(2), facetDataFetcherFixedLengthLongArray, depends);
    }
    return null;
  }



  public static FacetDataFetcher facetDataFetcher = new FacetDataFetcher()
  {
    @Override
    public Object fetch(BoboIndexReader reader, int doc)
    {
      FacetDataCache dataCache = (FacetDataCache)reader.getFacetData("groupid");
      long ret =  (Long) dataCache.valArray.getRawValue(dataCache.orderArray.get(doc));
      if (ret < 0) ret *= -1;
      return ret;
    }

    @Override
    public void cleanup(BoboIndexReader reader)
    {
    }
  };

  public static class GroupIdFetcherFactory implements SenseiPluginFactory<FacetDataFetcher>
  {
    public FacetDataFetcher getBean(Map<String, String> initProperties,
                                    String fullPrefix,
                                    SenseiPluginRegistry pluginRegistry)
    {
      return VirtualGroupIdFactory.facetDataFetcher;
    }
  }

  public static FacetDataFetcher facetDataFetcherFixedLengthLongArray = new FacetDataFetcher()
  {
    private int counter = 0;

    @Override
    public Object fetch(BoboIndexReader reader, int doc)
    {
      FacetDataCache dataCache = (FacetDataCache)reader.getFacetData("groupid");
      long[] val = new long[2];
      val[0] = counter%5;
      ++counter;
      Long groupId = (Long)dataCache.valArray.getRawValue(dataCache.orderArray.get(doc));
      if (groupId < 0) groupId *= -1;
      if (groupId == null)
        val[1] = 0;
      else
        val[1] = groupId;
      
      return val;
    }

    @Override
    public void cleanup(BoboIndexReader reader)
    {
      counter = 0;
    }
  };
}
