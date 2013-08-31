package com.senseidb.facet;


import com.senseidb.facet.handler.FacetHandler;
import com.senseidb.facet.handler.FacetHandlerInitializerParam;
import com.senseidb.facet.handler.RuntimeFacetHandler;
import com.senseidb.facet.handler.RuntimeFacetHandlerFactory;
import com.senseidb.facet.filter.AndFilter;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author Dmytro Ivchenko
 */
public class FacetRequest {

  private final FacetRequestParams _params;
  private final FacetSystem _system;
  private HashMap<String, FacetHandler> _allFacetHandlerMap;
  private HashMap<String, RuntimeFacetHandler<?>> _runtimeFacetHandlerMap;

  public FacetRequest(FacetSystem system) throws IOException {
    this(system, new FacetRequestParams());
  }

  public FacetRequest(FacetSystem system, FacetRequestParams params) throws IOException {
    _params = params;
    _system = system;

    _allFacetHandlerMap = new HashMap<String, FacetHandler>();
    for (Map.Entry<String, FacetHandler> entry : _system.getFacetHandlerMap().entrySet()) {
      _allFacetHandlerMap.put(entry.getKey(), entry.getValue());
    }

    // Initialize all RuntimeFacetHandlers with data supplied by user at run-time.
    _runtimeFacetHandlerMap = new HashMap<String, RuntimeFacetHandler<?>>();
    for (RuntimeFacetHandlerFactory factory : _system.getRuntimeFactoryMap().values()) {
      FacetHandlerInitializerParam data = _params.getFacethandlerData(factory.getName());
      if (data == null)
        data = FacetHandlerInitializerParam.EMPTY_PARAM;

      if (_params.getFacetSpec(factory.getName()) != null ||
          data != FacetHandlerInitializerParam.EMPTY_PARAM || !factory.initParamsRequired()) {
        RuntimeFacetHandler<?> facetHandler = factory.get(data);
        _allFacetHandlerMap.put(factory.getName(), facetHandler);
        _runtimeFacetHandlerMap.put(factory.getName(), facetHandler);

        processDependencies(facetHandler);
      }
    }
  }

  public FacetRequestParams getParams() {
    return _params;
  }

  public HashMap<String, FacetHandler> getAllFacetHandlerMap() {
    return _allFacetHandlerMap;
  }

  public HashMap<String, RuntimeFacetHandler<?>> getRuntimeFacetHandlerMap() {
    return _runtimeFacetHandlerMap;
  }

  public Query newQuery(Query query) throws IOException {
    List<Filter> filters = new ArrayList<Filter>();
    for (Map.Entry<String, FacetHandler> entry : _allFacetHandlerMap.entrySet()) {
      FacetSpec spec = _params.getFacetSpec(entry.getKey());
      FacetSelection sel = _params.getSelection(entry.getKey());
      if (spec != null && spec.isExpandSelection() && sel != null) {
        Filter filter = entry.getValue().buildFilter(sel);
        if (filter != null)
          filters.add(filter);
      }
    }

    if (filters.size() > 0) {
      return new FilteredQuery(query, new AndFilter(filters));
    } else {
      return query;
    }
  }

  public FacetCollector newCollector(Collector collector) {
    return new FacetCollector(collector, this);
  }

  private void processDependencies(RuntimeFacetHandler<?> facetHandler) throws IOException {
    _runtimeFacetHandlerMap.put(facetHandler.getName(), facetHandler);
    _allFacetHandlerMap.put(facetHandler.getName(), facetHandler);

    for (String name : facetHandler.getDependsOn()) {
      if (!_allFacetHandlerMap.containsKey(name)) {
        RuntimeFacetHandlerFactory factory = _system.getRuntimeFactoryMap().get(name);
        if (null == factory) {
          throw new IOException("Dependent runtime facet handler factory not found, " + name);
        }
        if (!factory.initParamsRequired()) {
          throw new IOException("Dependent runtime facet handler factory can't be loaded lazily, " + name);
        }

        RuntimeFacetHandler depHandler = factory.get(FacetHandlerInitializerParam.EMPTY_PARAM);
        _runtimeFacetHandlerMap.put(factory.getName(), depHandler);
        _allFacetHandlerMap.put(factory.getName(), depHandler);
      }
    }
  }
}
