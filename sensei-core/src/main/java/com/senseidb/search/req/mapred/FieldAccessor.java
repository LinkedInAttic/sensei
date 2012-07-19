package com.senseidb.search.req.mapred;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import proj.zoie.api.DocIDMapper;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermNumberList;
import com.browseengine.bobo.facets.data.TermShortList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.search.req.SenseiSystemInfo.SenseiFacetInfo;

/**
 * This class was designed to avoid polymorphism and to leverage primitive types as much as possible
 * It allows  access to the facetted data
 *
 */
@SuppressWarnings("rawtypes")
public final class FieldAccessor  {
  private final Set<String> facets = new HashSet<String>();
  private final BoboIndexReader boboIndexReader;
  private FacetDataCache lastFacetDataCache;
  private String lastFacetDataCacheName;
  
  private Map<String, FacetDataCache> facetDataMap = new HashMap<String, FacetDataCache>();  
  
  private final DocIDMapper mapper;
  
 
  public FieldAccessor(Set<SenseiFacetInfo> facetInfos, BoboIndexReader boboIndexReader, DocIDMapper mapper) {    
    this.mapper = mapper;
    for (SenseiFacetInfo facetInfo : facetInfos) {
      facets.add(facetInfo.getName());
    }    
    this.boboIndexReader = boboIndexReader;    
  }
  public  final FacetDataCache getValueCache(String name) {
    if (name.equals(lastFacetDataCacheName)) {
      return  lastFacetDataCache;
    }
    FacetDataCache ret = facetDataMap.get(name);
    if (ret != null) {
      lastFacetDataCache = ret;
      lastFacetDataCacheName = name;
      return ret;
    }
    
    Object rawFacetData = boboIndexReader.getFacetData(name);   
    if (!(rawFacetData instanceof FacetDataCache)) {     
      return null;
    }
    ret = (FacetDataCache) rawFacetData;
    facetDataMap.put(name, ret); 
    return ret;
  }
  
  
  
  /**
   * Get facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public final  Object get(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);
    if (valueCache instanceof MultiValueFacetDataCache) {
      return getArray(fieldName, docId);
    }
    if (valueCache != null) {
      return valueCache.valArray.getRawValue(valueCache.orderArray.get(docId));
    }
    return getFacetHandler(fieldName).getRawFieldValues(boboIndexReader, docId);
  }

  /**
   * Get string facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public final String getString(String fieldName, int docId) {
    return getFacetHandler(fieldName).getFieldValue(boboIndexReader, docId);    
  }

  /**
   * Get long  facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public final long getLong(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);    
    if (valueCache != null) {
      if (valueCache.valArray instanceof TermLongList) {
        return ((TermLongList) valueCache.valArray).getPrimitiveValue(valueCache.orderArray.get(docId));
      } else {
        return (long)((TermNumberList) valueCache.valArray).getDoubleValue(valueCache.orderArray.get(docId));
      }
    } else {
      Object value = getFacetHandler(fieldName).getRawFieldValues(boboIndexReader, docId)[0];
      if (value instanceof Long) {
        return (Long)value;
      }
      if (value instanceof Number) {
        return ((Number)value).longValue();
      }
      if (value instanceof String) {
        return Long.parseLong((String)value);
      }
      throw new UnsupportedOperationException("Class " + value.getClass() + " can not be converted to long");
    }
  }

  /**
   * Get double  facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public final double getDouble(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);    
    if (valueCache != null) {
      return ((TermNumberList) valueCache.valArray).getDoubleValue(valueCache.orderArray.get(docId));
    } else {
      Object value = getFacetHandler(fieldName).getRawFieldValues(boboIndexReader, docId)[0];
      if (value instanceof Double) {
        return (Double)value;
      }
      if (value instanceof Number) {
        return ((Number)value).doubleValue();
      }
      if (value instanceof String) {
        return Double.parseDouble((String)value);
      }
      throw new UnsupportedOperationException("Class " + value.getClass() + " can not be converted to double");
    }
  }

  /**
   * Get short  facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public final short getShort(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);   
    if (valueCache != null) {
      if (valueCache.valArray instanceof TermShortList) {
        return ((TermShortList) valueCache.valArray).getPrimitiveValue(valueCache.orderArray.get(docId));
      } else {
        return (short)((TermNumberList) valueCache.valArray).getDoubleValue(valueCache.orderArray.get(docId));
      }
    } else {
      Object value = getFacetHandler(fieldName).getRawFieldValues(boboIndexReader, docId)[0];
      if (value instanceof Short) {
        return (Short)value;
      }
      if (value instanceof Number) {
        return ((Number)value).shortValue();
      }
      if (value instanceof String) {
        return Short.parseShort((String)value);
      }
      throw new UnsupportedOperationException("Class " + value.getClass() + " can not be converted to short");
    }
  }

  /**
   * Get integer  facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public final int getInteger(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);    
    if (valueCache != null) {
      if (valueCache.valArray instanceof TermIntList) {
        return ((TermIntList) valueCache.valArray).getPrimitiveValue(valueCache.orderArray.get(docId));
      } else {
        return (int)((TermNumberList) valueCache.valArray).getDoubleValue(valueCache.orderArray.get(docId));
      }
    } else {
      Object value = getFacetHandler(fieldName).getRawFieldValues(boboIndexReader, docId)[0];
      if (value instanceof Integer) {
        return (Integer)value;
      }
      if (value instanceof Number) {
        return ((Number)value).intValue();
      }
      if (value instanceof String) {
        return Integer.parseInt((String)value);
      }
      throw new UnsupportedOperationException("Class " + value.getClass() + " can not be converted to int");
    }
  }

  /**
   * Get float  facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public final float getFloat(String fieldName, int docId) {
    FacetDataCache valueCache = getValueCache(fieldName);    
    if (valueCache != null) {
      if (valueCache.valArray instanceof TermFloatList) {
        return ((TermFloatList) valueCache.valArray).getPrimitiveValue(valueCache.orderArray.get(docId));
      } else {
        return (float)((TermNumberList) valueCache.valArray).getDoubleValue(valueCache.orderArray.get(docId));
      }
    } else {
      Object value = getFacetHandler(fieldName).getRawFieldValues(boboIndexReader, docId)[0];
      if (value instanceof Float) {
        return (Float)value;
      }
      if (value instanceof Number) {
        return ((Number)value).floatValue();
      }
      if (value instanceof String) {
        return Float.parseFloat((String)value);
      }
      throw new UnsupportedOperationException("Class " + value.getClass() + " can not be converted to float");
    }
  }

  /**
   * Get array  facet value for the document
   * @param fieldName
   * @param docId
   * @return
   */
  public final Object[] getArray(String fieldName, int docId) {
    return getFacetHandler(fieldName).getRawFieldValues(boboIndexReader, docId); 
   
  }
  
  public final Object getByUID(String fieldName, long uid) {    
    return get(fieldName, mapper.quickGetDocID(uid));
  }
  
  public final String getStringByUID(String fieldName, long uid) {
    return getString(fieldName, mapper.quickGetDocID(uid));
  }
  
  public final long getLongByUID(String fieldName, long uid) {
    return getLong(fieldName, mapper.quickGetDocID(uid));
  }
  
  public final double getDoubleByUID(String fieldName, long uid) {
    return getDouble(fieldName, mapper.quickGetDocID(uid));
  }
  
  public final short getShortByUID(String fieldName, long uid) {
    return getShort(fieldName, mapper.quickGetDocID(uid));
  }
  
  public final int getIntegerByUID(String fieldName, long uid) {
    return getInteger(fieldName, mapper.quickGetDocID(uid));
  }
  
  public final float getFloatByUID(String fieldName, long uid) {
    return getFloat(fieldName, mapper.quickGetDocID(uid));
  }
  
  public final Object[] getArrayByUID(String fieldName, long uid) {
    return getArray(fieldName, mapper.quickGetDocID(uid));
  }
  
  public final TermValueList getTermValueList(String fieldName) {    
     FacetDataCache valueCache = getValueCache(fieldName);
     if (valueCache == null) {
       return null;
     }
     return valueCache.valArray;
  }
  private String lastFacetHandlerName;
  private FacetHandler lastFacetHandler;
  
  /**
   * @param facetName
   * @return
   * @throws IllegalStateException if the facet can not be found
   */
  public final FacetHandler getFacetHandler(String facetName) {    
    if (!facetName.equals(lastFacetHandlerName)) {
      lastFacetHandler = boboIndexReader.getFacetHandler(facetName);
      lastFacetHandlerName = facetName;
    }
    if (lastFacetHandler == null) {
      throw new IllegalStateException("The facetHandler - " + facetName + " is not defined in the schema");
    }
    return lastFacetHandler;
  }
  public BoboIndexReader getBoboIndexReader() {
    return boboIndexReader;
  }
  /**
   * Returns the docIdtoUID mapper
   * @return
   */
  public DocIDMapper getMapper() {
    return mapper;
  }

}
