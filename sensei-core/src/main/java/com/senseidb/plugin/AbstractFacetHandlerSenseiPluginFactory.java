package com.senseidb.plugin;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.browseengine.bobo.facets.data.TermListFactory;
import com.browseengine.bobo.facets.FacetHandler;

public abstract class AbstractFacetHandlerSenseiPluginFactory implements SenseiPluginFactory<FacetHandler>
{
  public static final String DEPENDS = "depends";

  public Set<String> getDepends(Map<String, String> initProperties)
  {
    Set<String> depends = new HashSet<String>();

    String val = initProperties.get(DEPENDS);
    if (val != null)
    {
      for (String depend : val.split(","))
      {
        depend = depend.trim();
        if (depend.length() != 0)
          depends.add(depend);
      }
    }

    return depends;
  }

  @Override
  public abstract FacetHandler getBean(Map<String,String> initProperties,
                                       String fullPrefix,
                                       SenseiPluginRegistry pluginRegistry);
}
