package com.senseidb.test.plugin;

import com.browseengine.bobo.facets.FacetHandlerInitializerParam;
import com.browseengine.bobo.facets.RuntimeFacetHandler;
import com.browseengine.bobo.facets.RuntimeFacetHandlerFactory;

public class MockRuntimeFacetHandlerFactory implements RuntimeFacetHandlerFactory<FacetHandlerInitializerParam, RuntimeFacetHandler<?>> {

  @Override
  public String getName() {
    return "mockHandlerFactory";
  }

  @Override
  public RuntimeFacetHandler<?> get(FacetHandlerInitializerParam params) {
    
    return null;
  }

  @Override
  public boolean isLoadLazily() {
      return false;
  }

}
