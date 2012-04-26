package com.senseidb.test.plugin;

import java.util.HashSet;
import java.util.Map;

import com.linkedin.bobo.api.BoboIndexReader;
import com.linkedin.bobo.facets.data.FacetDataCache;
import com.linkedin.bobo.facets.data.FacetDataFetcher;
import com.linkedin.bobo.facets.data.PredefinedTermListFactory;
import com.linkedin.bobo.facets.data.TermFixedLengthLongArrayListFactory;
import com.linkedin.bobo.facets.impl.VirtualSimpleFacetHandler;
import com.senseidb.plugin.SenseiPluginFactory;
import com.senseidb.plugin.SenseiPluginRegistry;

import com.linkedin.zoie.api.ZoieIndexReader;

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
    @Override
    public Object fetch(BoboIndexReader reader, int doc)
    {
      long uid = ((ZoieIndexReader)reader.getInnerReader()).getUID(doc);
      long[] val = new long[2];
      val[0] = uid;
      if (uid%4 == 1) val[0] = val[0] - 1;
      if (uid%4 == 2) val[0] = val[0] - 2;
      val[1] = uid/10;
      
      return val;
    }

    @Override
    public void cleanup(BoboIndexReader reader)
    {
    }
  };
}
