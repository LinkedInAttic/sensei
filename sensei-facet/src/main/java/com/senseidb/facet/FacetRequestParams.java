package com.senseidb.facet;


import com.senseidb.facet.handler.FacetHandlerInitializerParam;
import org.apache.lucene.search.SortField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * @author Dmytro Ivchenko
 */
public class FacetRequestParams {

  private Map<String, FacetSelection> _selections;
  private ArrayList<SortField> _sortSpecs;
  private Map<String, FacetSpec> _facetSpecMap;
  private Map<String, FacetHandlerInitializerParam> _facetHandlerDataMap;
  private Set<String> _facetsToFetch;

  public FacetRequestParams() {
    _selections = new HashMap<String, FacetSelection>();
    _sortSpecs = new ArrayList<SortField>();
    _facetSpecMap = new HashMap<String, FacetSpec>();
    _facetHandlerDataMap = new HashMap<String, FacetHandlerInitializerParam>();
  }

  public FacetRequestParams setFacetSpec(String name, FacetSpec facetSpec) {
    _facetSpecMap.put(name, facetSpec);
    return this;
  }

  public FacetSpec getFacetSpec(String name) {
    return _facetSpecMap.get(name);
  }

  public FacetRequestParams setFacetHandlerData(String name, FacetHandlerInitializerParam data) {
    _facetHandlerDataMap.put(name, data);
    return this;
  }

  public FacetHandlerInitializerParam getFacethandlerData(String name) {
    return _facetHandlerDataMap.get(name);
  }

  public FacetRequestParams setSelection(FacetSelection sel) {
    List<String> vals = sel.getValues();
    if (vals == null || vals.size() == 0) {
      List<String> notVals = sel.getNotValues();
      if (notVals == null || notVals.size() == 0)
        return this;
    }
    _selections.put(sel.getFieldName(), sel);
    return this;
  }

  public FacetSelection getSelection(String field) {
    return _selections.get(field);
  }

  public FacetRequestParams addSortField(SortField sortSpec) {
    _sortSpecs.add(sortSpec);
    return this;
  }

  public List<SortField> getSort() {
    return _sortSpecs;
  }

  public FacetRequestParams addFacetToFetch(String name) {
    if (null == _facetsToFetch)
      _facetsToFetch = new HashSet<String>();
    _facetsToFetch.add(name);
    return this;
  }

  public Set<String> getFacetsToFetch() {
    return _facetsToFetch;
  }
}


