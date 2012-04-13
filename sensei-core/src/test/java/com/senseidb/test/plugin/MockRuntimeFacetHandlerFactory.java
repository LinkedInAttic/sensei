package com.senseidb.test.plugin;

import com.browseengine.bobo.facets.FacetHandlerInitializerParam;
import com.browseengine.bobo.facets.RuntimeFacetHandler;
import com.browseengine.bobo.facets.AbstractRuntimeFacetHandlerFactory;

public class MockRuntimeFacetHandlerFactory extends AbstractRuntimeFacetHandlerFactory<FacetHandlerInitializerParam, RuntimeFacetHandler<?>> {

  @Override
  public String getName() {
    return "mockHandlerFactory";
  }

  @Override
  public RuntimeFacetHandler<?> get(FacetHandlerInitializerParam params) {
    
    return null;
  }
}
