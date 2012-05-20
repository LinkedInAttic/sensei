package com.senseidb.test.plugin;

import com.linkedin.bobo.facets.FacetHandlerInitializerParam;
import com.linkedin.bobo.facets.RuntimeFacetHandler;
import com.linkedin.bobo.facets.AbstractRuntimeFacetHandlerFactory;

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
