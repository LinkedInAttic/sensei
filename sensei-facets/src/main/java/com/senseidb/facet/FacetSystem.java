package com.senseidb.facet;


import com.senseidb.facet.handler.FacetHandler;
import com.senseidb.facet.handler.RuntimeFacetHandlerFactory;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author Dmytro Ivchenko
 */
public class FacetSystem {

  private final Map<String, FacetHandler> _facetHandlerMap;
  private final Map<String, RuntimeFacetHandlerFactory<?, ?>> _runtimeFactoryMap;

  public FacetSystem(List<FacetHandler> facetHandlers) {
    this(facetHandlers, null);
  }

  public FacetSystem(List<FacetHandler> facetHandlers, List<RuntimeFacetHandlerFactory<?, ?>> runtimeFactories) {
    _facetHandlerMap = new HashMap<String, FacetHandler>();
    for (FacetHandler handler : facetHandlers) {
      _facetHandlerMap.put(handler.getName(), handler);
    }

    _runtimeFactoryMap = new HashMap<String, RuntimeFacetHandlerFactory<?, ?>>();
    if (runtimeFactories != null) {
      for (RuntimeFacetHandlerFactory factory : runtimeFactories) {
        _runtimeFactoryMap.put(factory.getName(), factory);
      }
    }
  }

  public Map<String, FacetHandler> getFacetHandlerMap() {
    return _facetHandlerMap;
  }

  public Map<String, RuntimeFacetHandlerFactory<?, ?>> getRuntimeFactoryMap() {
    return _runtimeFactoryMap;
  }
}
