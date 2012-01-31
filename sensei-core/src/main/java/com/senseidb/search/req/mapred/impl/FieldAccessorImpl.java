package com.senseidb.search.req.mapred.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import proj.zoie.api.DocIDMapper;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.data.TermDoubleList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermShortList;
import com.senseidb.search.req.SenseiSystemInfo.SenseiFacetInfo;
import com.senseidb.search.req.mapred.FieldAccessor;

public class FieldAccessorImpl implements FieldAccessor {
  private final Set<String> facets = new HashSet<String>();
  private final BoboIndexReader boboIndexReader;
  
  private Map<String, FacetDataCache> facetDataMap = new HashMap<String, FacetDataCache>();  
  private Set<String> unsupportedFacets = new HashSet<String>();
  private final DocIDMapper mapper;
  
  @SuppressWarnings("rawtypes")
  public FieldAccessorImpl(Set<SenseiFacetInfo> facetInfos, BoboIndexReader boboIndexReader, DocIDMapper mapper) {    
    this.mapper = mapper;
    for (SenseiFacetInfo facetInfo : facetInfos) {
      facets.add(facetInfo.getName());
    }    
    this.boboIndexReader = boboIndexReader;    
  }
  public final FacetDataCache getValueCache(String name) {
    FacetDataCache ret = facetDataMap.get(name);
    if (ret != null) {
      return ret;
    }
    if (unsupportedFacets.contains(name)) {
      throw new IllegalStateException("The field retrieval is unsupported for the facetHandler " + name);
    }
    Object rawFacetData = boboIndexReader.getFacetData(name);   
    if (!(rawFacetData instanceof FacetDataCache)) {
      unsupportedFacets.add(name);
      throw new IllegalStateException("The field retrieval is unsupported for the facetHandler " + name);
    }
    ret = (FacetDataCache) rawFacetData;
    facetDataMap.put(name, ret); 
    return ret;
  }
  
  
  @Override
  public Object get(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);
    if (valueCache instanceof MultiValueFacetDataCache) {
      return getArray(fieldName, docId);
    }
    return valueCache.valArray.getInnerList().get(valueCache.orderArray.get(docId));
  }

  @Override
  public String getString(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);
    if (valueCache instanceof MultiValueFacetDataCache) {
      throw new IllegalStateException("Field " + fieldName + " is the multiValueField");
    }
    return valueCache.valArray.get(valueCache.orderArray.get(docId));
  }

  @Override
  public long getLong(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);
    if (valueCache instanceof MultiValueFacetDataCache) {
      throw new IllegalStateException("Field " + fieldName + " is the multiValueField");
    }
    return ((TermLongList) valueCache.valArray).getPrimitiveValue(valueCache.orderArray.get(docId));
  }

  @Override
  public double getDouble(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);
    if (valueCache instanceof MultiValueFacetDataCache) {
      throw new IllegalStateException("Field " + fieldName + " is the multiValueField");
    }
    return ((TermDoubleList) valueCache.valArray).getPrimitiveValue(valueCache.orderArray.get(docId));
  }

  @Override
  public short getShort(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);
    if (valueCache instanceof MultiValueFacetDataCache) {
      throw new IllegalStateException("Field " + fieldName + " is the multiValueField");
    }
    return ((TermShortList) valueCache.valArray).getPrimitiveValue(valueCache.orderArray.get(docId));
  }

  @Override
  public int getInteger(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);
    if (valueCache instanceof MultiValueFacetDataCache) {
      throw new IllegalStateException("Field " + fieldName + " is the multiValueField");
    }
    return ((TermIntList) valueCache.valArray).getPrimitiveValue(valueCache.orderArray.get(docId));
  }

  @Override
  public float getFloat(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);
    if (valueCache instanceof MultiValueFacetDataCache) {
      throw new IllegalStateException("Field " + fieldName + " is the multiValueField");
    }
    return ((TermIntList) valueCache.valArray).getPrimitiveValue(valueCache.orderArray.get(docId));
  }

  @Override
  public Object[] getArray(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);
    if (valueCache instanceof MultiValueFacetDataCache) {
      return ((MultiValueFacetDataCache)valueCache)._nestedArray.getRawData(docId, valueCache.valArray);
    }
    return new Object[]{valueCache.valArray.getInnerList().get(valueCache.orderArray.get(docId))};
  }
  @Override
  public Object getByUID(String fieldName, long uid) {    
    return get(fieldName, mapper.quickGetDocID(uid));
  }
  @Override
  public String getStringByUID(String fieldName, long uid) {
    return getString(fieldName, mapper.quickGetDocID(uid));
  }
  @Override
  public long getLongByUID(String fieldName, long uid) {
    return getLong(fieldName, mapper.quickGetDocID(uid));
  }
  @Override
  public double getDoubleByUID(String fieldName, long uid) {
    return getDouble(fieldName, mapper.quickGetDocID(uid));
  }
  @Override
  public short getShortByUID(String fieldName, long uid) {
    return getShort(fieldName, mapper.quickGetDocID(uid));
  }
  @Override
  public int getIntegerByUID(String fieldName, long uid) {
    return getInteger(fieldName, mapper.quickGetDocID(uid));
  }
  @Override
  public float getFloatByUID(String fieldName, long uid) {
    return getFloat(fieldName, mapper.quickGetDocID(uid));
  }
  @Override
  public Object[] getArrayByUID(String fieldName, long uid) {
    return getArray(fieldName, mapper.quickGetDocID(uid));
  }

}
