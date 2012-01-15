package com.senseidb.test.plugin;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.PredefinedTermListFactory;
import com.browseengine.bobo.facets.impl.SimpleFacetHandler;
import com.senseidb.plugin.SenseiPluginFactory;
import com.senseidb.plugin.SenseiPluginRegistry;

public class OtherCustomFacetsFactory implements SenseiPluginFactory<List<FacetHandler<?>>>{

  @Override
  public List<FacetHandler<?>> getBean(Map<String, String> initProperties, String fullPrefix,
      SenseiPluginRegistry pluginRegistry) {
    List<FacetHandler<?>> ret = new ArrayList<FacetHandler<?>>();
    ret.add(new SimpleFacetHandler("handler1", "field1" , new PredefinedTermListFactory(Long.class), new HashSet<String>()));
    ret.add(new SimpleFacetHandler("handler2", "field2" , new PredefinedTermListFactory(Long.class), new HashSet<String>()));
    ret.add(new SimpleFacetHandler("handler3", "field3" , new PredefinedTermListFactory(Long.class), new HashSet<String>()));
    return ret;
  }

}
