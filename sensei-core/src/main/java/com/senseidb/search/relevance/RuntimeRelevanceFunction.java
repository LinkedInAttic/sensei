package com.senseidb.search.relevance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.data.TermDoubleList;
import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermShortList;
import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.util.BigSegmentedArray;
import com.senseidb.search.query.ScoreAugmentQuery.ScoreAugmentFunction;
import com.senseidb.search.relevance.impl.CompilationHelper;
import com.senseidb.search.relevance.impl.CustomMathModel;
import com.senseidb.search.relevance.impl.RelevanceJSONConstants;
import com.senseidb.search.relevance.impl.MFacetDouble;
import com.senseidb.search.relevance.impl.MFacetFloat;
import com.senseidb.search.relevance.impl.MFacetInt;
import com.senseidb.search.relevance.impl.MFacetLong;
import com.senseidb.search.relevance.impl.MFacetShort;
import com.senseidb.search.relevance.impl.MFacetString;
import com.senseidb.search.relevance.impl.WeightedMFacetDouble;
import com.senseidb.search.relevance.impl.WeightedMFacetFloat;
import com.senseidb.search.relevance.impl.WeightedMFacetInt;
import com.senseidb.search.relevance.impl.WeightedMFacetLong;
import com.senseidb.search.relevance.impl.WeightedMFacetShort;
import com.senseidb.search.relevance.impl.WeightedMFacetString;
import com.senseidb.search.relevance.impl.CompilationHelper.DataTable;

public class RuntimeRelevanceFunction extends CustomRelevanceFunction
{
  
  private static Logger logger = Logger.getLogger(RuntimeRelevanceFunction.class);
  
  //per request shared data;
  private DataTable _dt;
  private CustomMathModel _cModel;
  
  
  //index reader level data;
  private BigSegmentedArray[] _orderArrays;
  private TermValueList[] _termLists;
  
  private MultiValueFacetDataCache[] _mDataCaches;
  private TermValueList[] _mTermLists;
  
  private int[] _types;
  private int[] _facetIndex;
  private int[] _arrayIndex;
  
  private int[] _mFacetIndex;
  private int[] _mArrayIndex;
  
  private int _paramSize;
  
  
  private short[] shorts;
  private int[] ints;
  private long[] longs;
  private float[] floats;
  private double[] doubles;
  private boolean[] booleans;
  private String[] strings;
  private Set[] sets;
  private Map[] maps;
  
  private MFacetInt[] mFacetInts;
  private MFacetLong[] mFacetLongs;
  private MFacetShort[] mFacetShorts;
  private MFacetFloat[] mFacetFloats;
  private MFacetDouble[] mFacetDoubles;
  private MFacetString[] mFacetStrings;
  
  private int[] dynamicAR;
  
  
  public RuntimeRelevanceFunction(CustomMathModel cModel, 
                       DataTable dt)
  {
    _cModel = cModel;
    _dt = dt;
  }
  
  
  private void initialRunningData(BoboIndexReader boboReader, 
                       CustomMathModel cModel, 
                       DataTable _dt) throws IOException
  {
    int numFacet = _dt.hm_symbol_facet.keySet().size();
    final BigSegmentedArray[] orderArrays = new BigSegmentedArray[numFacet];
    final TermValueList[] termLists = new TermValueList[numFacet];
    
    Iterator<String> iter_facet = _dt.hm_facet_index.keySet().iterator();
    while(iter_facet.hasNext()){
      String facetName = iter_facet.next();
      
      // validation;
      Object dataObj = boboReader.getFacetData(facetName);
      if ( ! (dataObj instanceof FacetDataCache<?>))
        throw new IllegalArgumentException("Facet " + facetName + " does not have a valid FacetDataCache");
      
      int index = _dt.hm_facet_index.get(facetName);
      orderArrays[index] = ((FacetDataCache)(boboReader.getFacetData(facetName))).orderArray;
      termLists[index] = ((FacetDataCache)(boboReader.getFacetData(facetName))).valArray;
    }

    //multi-facet;
    int numMultiFacet = _dt.hm_symbol_mfacet.keySet().size();
    final MultiValueFacetDataCache[] mDataCaches = new MultiValueFacetDataCache[numMultiFacet];
    final TermValueList[] mTermLists = new TermValueList[numMultiFacet];
    
    Iterator<String> iter_mfacet = _dt.hm_mfacet_index.keySet().iterator();
    while(iter_mfacet.hasNext()){
      String mFacetName = iter_mfacet.next();
      
      // validation;
      Object dataObj = boboReader.getFacetData(mFacetName);
      if ( ! (dataObj instanceof FacetDataCache<?>))
        throw new IllegalArgumentException("Facet " + mFacetName + " does not have a valid FacetDataCache");
      
      int index = _dt.hm_mfacet_index.get(mFacetName);
      mDataCaches[index] = (MultiValueFacetDataCache)(boboReader.getFacetData(mFacetName));
      mTermLists[index] = ((MultiValueFacetDataCache)(boboReader.getFacetData(mFacetName))).valArray;
    }
    
    
    final int paramSize = _dt.lls_params.size();
    
    final int[] types = new int[paramSize];  //store each parameter's type;
    final int[] facetIndex = new int[paramSize];  // if this parameter is a facet, what is its index number in the facet data array;
    final int[] arrayIndex = new int[paramSize];  // for each paramter, what is its index number in its own parameter array when passing into the function;
    final int[] mFacetIndex = new int[paramSize];  // if this parameter is a multi-facet, we need to know its index. Since we only use one array to store multi-facet, we do not need array index like the one for the simple facet;
    final int[] mArrayIndex = new int[paramSize];  // for each multi-facet, what is its index number in its own parameter array when passing into the function;
    
    updateArrayIndex(_dt, paramSize, types, facetIndex, arrayIndex, mFacetIndex, mArrayIndex);
    
    
    
    /////
    
    _cModel = cModel;
    _orderArrays = orderArrays;
    _termLists = termLists;
    _types = types;
    _facetIndex = facetIndex;
    _arrayIndex = arrayIndex;
    
    _mDataCaches = mDataCaches;
    _mTermLists = mTermLists;
    _mFacetIndex = mFacetIndex;
    _mArrayIndex = mArrayIndex;
    
    _paramSize = paramSize;
    
    shorts    = new short[_paramSize];
    ints      = new int[_paramSize];
    longs     = new long[_paramSize];
    floats    = new float[_paramSize];
    doubles   = new double[_paramSize];
    booleans  = new boolean[_paramSize];
    strings   = new String[_paramSize];
    sets      = new Set[_paramSize];
    maps      = new Map[_paramSize];
    
    mFacetInts   = new MFacetInt[_paramSize];
    mFacetLongs = new MFacetLong[_paramSize] ;
    mFacetShorts = new MFacetShort[_paramSize] ;
    mFacetFloats = new MFacetFloat[_paramSize];
    mFacetDoubles = new MFacetDouble[_paramSize];
    mFacetStrings = new MFacetString[_paramSize];
    
    
    ArrayList<Integer> arDynamic = new ArrayList<Integer>();
    
    // prepare the static variable;
    for(int i=0; i<_paramSize; i++)
    {
      switch (_types[i]) {
        case RelevanceJSONConstants.TYPENUMBER_INT:  
                  ints[_arrayIndex[i]] = ((Integer)_dt.hm_var.get(_dt.lls_params.get(i))).intValue();
                  break;
        case RelevanceJSONConstants.TYPENUMBER_LONG:
                  longs[_arrayIndex[i]] = ((Long)_dt.hm_var.get(_dt.lls_params.get(i))).longValue();
                  break;
        case RelevanceJSONConstants.TYPENUMBER_DOUBLE:  
                  doubles[_arrayIndex[i]] = ((Double)_dt.hm_var.get(_dt.lls_params.get(i))).doubleValue();
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FLOAT: 
                  floats[_arrayIndex[i]] = ((Float)_dt.hm_var.get(_dt.lls_params.get(i))).floatValue();
                  break;
        case RelevanceJSONConstants.TYPENUMBER_BOOLEAN: 
                  booleans[_arrayIndex[i]] = ((Boolean)_dt.hm_var.get(_dt.lls_params.get(i))).booleanValue();
                  break;
        case RelevanceJSONConstants.TYPENUMBER_STRING:
                  strings[_arrayIndex[i]] = (String) _dt.hm_var.get(_dt.lls_params.get(i));
                  break;
        case RelevanceJSONConstants.TYPENUMBER_SET:
                  sets[_arrayIndex[i]] = (Set)_dt.hm_var.get(_dt.lls_params.get(i));
                  break;
        case RelevanceJSONConstants.TYPENUMBER_MAP:
                  maps[_arrayIndex[i]] = (Map)_dt.hm_var.get(_dt.lls_params.get(i));
                  break;                    
                  
        
        //multi-facet container initialization; 
        case RelevanceJSONConstants.TYPENUMBER_FACET_M_INT:
                  mFacetInts[_mArrayIndex[i]] =  new MFacetInt(_mDataCaches[_mFacetIndex[i]]);
                  arDynamic.add(i);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_M_LONG:
                  mFacetLongs[_mArrayIndex[i]] =  new MFacetLong(_mDataCaches[_mFacetIndex[i]]);
                  arDynamic.add(i);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_M_DOUBLE:
                  mFacetDoubles[_mArrayIndex[i]] =  new MFacetDouble(_mDataCaches[_mFacetIndex[i]]);
                  arDynamic.add(i);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_M_FLOAT:
                  mFacetFloats[_mArrayIndex[i]] =  new MFacetFloat(_mDataCaches[_mFacetIndex[i]]);
                  arDynamic.add(i);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_M_SHORT:
                  mFacetShorts[_mArrayIndex[i]] =  new MFacetShort(_mDataCaches[_mFacetIndex[i]]);
                  arDynamic.add(i);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_M_STRING:                    
                  mFacetStrings[_mArrayIndex[i]] =  new MFacetString(_mDataCaches[_mFacetIndex[i]]);
                  arDynamic.add(i);
                  break;    
                  
        
        //weighted multi-facet container initialization; 
        case RelevanceJSONConstants.TYPENUMBER_FACET_WM_INT:
                  mFacetInts[_mArrayIndex[i]] =  new WeightedMFacetInt(_mDataCaches[_mFacetIndex[i]]);
                  arDynamic.add(i);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_WM_LONG:
                  mFacetLongs[_mArrayIndex[i]] =  new WeightedMFacetLong(_mDataCaches[_mFacetIndex[i]]);
                  arDynamic.add(i);
                  break;
        case  RelevanceJSONConstants.TYPENUMBER_FACET_WM_DOUBLE:
                  mFacetDoubles[_mArrayIndex[i]] =  new WeightedMFacetDouble(_mDataCaches[_mFacetIndex[i]]);
                  arDynamic.add(i);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_WM_FLOAT:
                  mFacetFloats[_mArrayIndex[i]] =  new WeightedMFacetFloat(_mDataCaches[_mFacetIndex[i]]);
                  arDynamic.add(i);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_WM_SHORT:
                  mFacetShorts[_mArrayIndex[i]] =  new WeightedMFacetShort(_mDataCaches[_mFacetIndex[i]]);
                  arDynamic.add(i);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_WM_STRING:                    
                  mFacetStrings[_mArrayIndex[i]] =  new WeightedMFacetString(_mDataCaches[_mFacetIndex[i]]);
                  arDynamic.add(i);
                  break;    
                  
                  
        default: 
                  arDynamic.add(i);
      }
    }
    
    dynamicAR = convertIntegers(arDynamic);
  }
  
  
  
  private int[] convertIntegers(List<Integer> integers)
  {
      int[] ret = new int[integers.size()];
      Iterator<Integer> iterator = integers.iterator();
      for (int i = 0; i < ret.length; i++)
      {
          ret[i] = iterator.next().intValue();
      }
      return ret;
  }
  
  private void updateArrayIndex(DataTable _dt, int paramSize, int[] types, int[] facetIndex, int[] arrayIndex, int[] mFacetIndex, int[] mArrayIndex)
  {
    int short_index = 0,    m_short_index = 0;
    int int_index = 0,      m_int_index = 0;
    int long_index = 0,     m_long_index = 0;
    int float_index = 0,    m_float_index = 0;
    int double_index = 0,   m_double_index = 0;
    int string_index = 0,   m_string_index = 0;
    
    int boolean_index = 0;
    
    int set_index = 0;
    int map_index = 0;

    for(int i=0; i< paramSize; i++)
    {
      boolean isMultiFacet = false;
      
      if(_dt.hm_type.get(_dt.lls_params.get(i)).equals(RelevanceJSONConstants.TYPE_INNER_SCORE)){
        types[i] = RelevanceJSONConstants.TYPENUMBER_INNER_SCORE;  //inner_score type parameter;
        facetIndex[i] = -1;  //should not be used;
        mFacetIndex[i] = -1;
        arrayIndex[i] = float_index;
        float_index++;
        mArrayIndex[i] = -1;
      }
      else if (_dt.hm_type.get(_dt.lls_params.get(i)).startsWith(RelevanceJSONConstants.TYPE_FACET_HEAD))
      {
        String type = _dt.hm_type.get(_dt.lls_params.get(i));
        
        if( (!type.startsWith(RelevanceJSONConstants.TYPE_M_FACET_HEAD)) && (!type.startsWith(RelevanceJSONConstants.TYPE_WM_FACET_HEAD)))
        {
          // non-multi-facet
          if(type.equals(RelevanceJSONConstants.TYPE_FACET_INT))
          {
            types[i] = RelevanceJSONConstants.TYPENUMBER_FACET_INT;
            arrayIndex[i] = int_index;
            int_index++;
          }
          else if (type.equals(RelevanceJSONConstants.TYPE_FACET_LONG))
          {
            types[i] = RelevanceJSONConstants.TYPENUMBER_FACET_LONG;
            arrayIndex[i] = long_index;
            long_index++;
          }
          else if (type.equals(RelevanceJSONConstants.TYPE_FACET_DOUBLE))
          {
            types[i] = RelevanceJSONConstants.TYPENUMBER_FACET_DOUBLE;
            arrayIndex[i] = double_index;
            double_index++;
          }
          else if (type.equals(RelevanceJSONConstants.TYPE_FACET_FLOAT))
          {
            types[i] = RelevanceJSONConstants.TYPENUMBER_FACET_FLOAT;
            arrayIndex[i] = float_index;
            float_index++;
          }
          else if (type.equals(RelevanceJSONConstants.TYPE_FACET_SHORT))
          {
            types[i] = RelevanceJSONConstants.TYPENUMBER_FACET_SHORT;
            arrayIndex[i] = short_index;
            short_index++;
          }
          else if (type.equals(RelevanceJSONConstants.TYPE_FACET_STRING))
          {
            types[i] = RelevanceJSONConstants.TYPENUMBER_FACET_STRING;
            arrayIndex[i] = string_index;
            string_index++;
          }
          
          mArrayIndex[i] = -1;
        }
        else 
        {
          // multi-facet or weighted multi-facet;
          isMultiFacet = true;
          
          //normal multi-facet
          if(type.equals(RelevanceJSONConstants.TYPE_FACET_M_INT))
          {
            types[i] = RelevanceJSONConstants.TYPENUMBER_FACET_M_INT;
            mArrayIndex[i] = m_int_index;
            m_int_index++;
          }
          else if (type.equals(RelevanceJSONConstants.TYPE_FACET_M_LONG))
          {
            types[i] = RelevanceJSONConstants.TYPENUMBER_FACET_M_LONG;
            mArrayIndex[i] = m_long_index;
            m_long_index++;
          }
          else if (type.equals(RelevanceJSONConstants.TYPE_FACET_M_DOUBLE))
          {
            types[i] = RelevanceJSONConstants.TYPENUMBER_FACET_M_DOUBLE;
            mArrayIndex[i] = m_double_index;
            m_double_index++;
          }
          else if (type.equals(RelevanceJSONConstants.TYPE_FACET_M_FLOAT))
          {
            types[i] = RelevanceJSONConstants.TYPENUMBER_FACET_M_FLOAT;
            mArrayIndex[i] = m_float_index;
            m_float_index++;
          }
          else if (type.equals(RelevanceJSONConstants.TYPE_FACET_M_SHORT))
          {
            types[i] = RelevanceJSONConstants.TYPENUMBER_FACET_M_SHORT;
            mArrayIndex[i] = m_short_index;
            m_short_index++;
          }
          else if (type.equals(RelevanceJSONConstants.TYPE_FACET_M_STRING))
          {
            types[i] = RelevanceJSONConstants.TYPENUMBER_FACET_M_STRING;
            mArrayIndex[i] = m_string_index;
            m_string_index++;
          }
          
          //weighted multi-facet
          else if(type.equals(RelevanceJSONConstants.TYPE_FACET_WM_INT))
          {
            types[i] = RelevanceJSONConstants.TYPENUMBER_FACET_WM_INT;
            mArrayIndex[i] = m_int_index;
            m_int_index++;
          }
          else if (type.equals(RelevanceJSONConstants.TYPE_FACET_WM_LONG))
          {
            types[i] = RelevanceJSONConstants.TYPENUMBER_FACET_WM_LONG;
            mArrayIndex[i] = m_long_index;
            m_long_index++;
          }
          else if (type.equals(RelevanceJSONConstants.TYPE_FACET_WM_DOUBLE))
          {
            types[i] = RelevanceJSONConstants.TYPENUMBER_FACET_WM_DOUBLE;
            mArrayIndex[i] = m_double_index;
            m_double_index++;
          }
          else if (type.equals(RelevanceJSONConstants.TYPE_FACET_WM_FLOAT))
          {
            types[i] = RelevanceJSONConstants.TYPENUMBER_FACET_WM_FLOAT;
            mArrayIndex[i] = m_float_index;
            m_float_index++;
          }
          else if (type.equals(RelevanceJSONConstants.TYPE_FACET_WM_SHORT))
          {
            types[i] = RelevanceJSONConstants.TYPENUMBER_FACET_WM_SHORT;
            mArrayIndex[i] = m_short_index;
            m_short_index++;
          }
          else if (type.equals(RelevanceJSONConstants.TYPE_FACET_WM_STRING))
          {
            types[i] = RelevanceJSONConstants.TYPENUMBER_FACET_WM_STRING;
            mArrayIndex[i] = m_string_index;
            m_string_index++;
          }
          
          arrayIndex[i] = -1;
        }
        
        if(isMultiFacet == false)
        {
          String facetName = _dt.hm_symbol_facet.get(_dt.lls_params.get(i));
          int index = _dt.hm_facet_index.get(facetName);
          facetIndex[i] = index;  // record the facet index;
          mFacetIndex[i] = -1;
        }
        else
        {
          String mfacetName = _dt.hm_symbol_mfacet.get(_dt.lls_params.get(i));
          int mIndex = _dt.hm_mfacet_index.get(mfacetName);
          facetIndex[i] = -1;
          mFacetIndex[i] = mIndex;  // record the multi-facet index;
        }
      }
      else
      {
        String type = _dt.hm_type.get(_dt.lls_params.get(i));  //normal type parameter;
        
        if(type.equals(RelevanceJSONConstants.TYPE_INT))
        {
          types[i] = RelevanceJSONConstants.TYPENUMBER_INT;
          arrayIndex[i] = int_index;
          int_index++;
        }
        else if (type.equals(RelevanceJSONConstants.TYPE_LONG))
        {
          types[i] = RelevanceJSONConstants.TYPENUMBER_LONG;
          arrayIndex[i] = long_index;
          long_index++;
        }
        else if (type.equals(RelevanceJSONConstants.TYPE_DOUBLE))
        {
          types[i] = RelevanceJSONConstants.TYPENUMBER_DOUBLE;
          arrayIndex[i] = double_index;
          double_index++;
        }
        else if (type.equals(RelevanceJSONConstants.TYPE_FLOAT))
        {
          types[i] = RelevanceJSONConstants.TYPENUMBER_FLOAT;
          arrayIndex[i] = float_index;
          float_index++;
        }
        else if (type.equals(RelevanceJSONConstants.TYPE_BOOLEAN))
        {
          types[i] = RelevanceJSONConstants.TYPENUMBER_BOOLEAN;
          arrayIndex[i] = boolean_index;
          boolean_index++;
        }
        else if (type.equals(RelevanceJSONConstants.TYPE_STRING))
        {
          types[i] = RelevanceJSONConstants.TYPENUMBER_STRING;
          arrayIndex[i] = string_index;
          string_index++;
        }
        else if (type.startsWith(RelevanceJSONConstants.TYPE_SET_HEAD))
        {
          types[i] = RelevanceJSONConstants.TYPENUMBER_SET;
          arrayIndex[i] = set_index;
          set_index++;
        }
        else if (type.startsWith(RelevanceJSONConstants.TYPE_MAP_HEAD))
        {
          types[i] = RelevanceJSONConstants.TYPENUMBER_MAP;
          arrayIndex[i] = map_index;
          map_index++;
        }
        
        facetIndex[i] = -1;  // should not be used;
        mFacetIndex[i] = -1;
        mArrayIndex[i] = -1;
      }
    }    
  }
  
  
  @Override
  public float newScore(float innerScore, int docID){
  
    //update the dynamic parameters only when we have to.
    for(int j=0; j < dynamicAR.length; j++)
    {
      
      // only when the parameter is inner score variable or facet variable, we need to update the score function input parameter arrays; 
      switch (_types[dynamicAR[j]]) {
        case RelevanceJSONConstants.TYPENUMBER_INNER_SCORE:  
                  floats[_arrayIndex[dynamicAR[j]]] = innerScore;
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_INT:  
                  ints[_arrayIndex[dynamicAR[j]]] = ((TermIntList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(docID));
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_LONG:
                  longs[_arrayIndex[dynamicAR[j]]] = ((TermLongList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(docID));
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_DOUBLE:  
                  doubles[_arrayIndex[dynamicAR[j]]] = ((TermDoubleList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(docID));
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_FLOAT: 
                  floats[_arrayIndex[dynamicAR[j]]] = ((TermFloatList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(docID));
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_SHORT: 
                  shorts[_arrayIndex[dynamicAR[j]]] = ((TermShortList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(docID));
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_STRING:
                  strings[_arrayIndex[dynamicAR[j]]] = ((TermStringList)_termLists[_facetIndex[dynamicAR[j]]]).get(_orderArrays[_facetIndex[dynamicAR[j]]].get(docID));
                  break;
                  
        // multi-facet below;
        case RelevanceJSONConstants.TYPENUMBER_FACET_M_INT:
                  mFacetInts[_mArrayIndex[dynamicAR[j]]].refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_M_LONG:
                  mFacetLongs[_mArrayIndex[dynamicAR[j]]].refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_M_DOUBLE:
                  mFacetDoubles[_mArrayIndex[dynamicAR[j]]].refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_M_FLOAT:
                  mFacetFloats[_mArrayIndex[dynamicAR[j]]].refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_M_SHORT:
                  mFacetShorts[_mArrayIndex[dynamicAR[j]]].refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_M_STRING:
                  mFacetStrings[_mArrayIndex[dynamicAR[j]]].refresh(docID);
                  break;

                  
        // weighted multi-facet below;
        case RelevanceJSONConstants.TYPENUMBER_FACET_WM_INT:
                  ((WeightedMFacetInt)mFacetInts[_mArrayIndex[dynamicAR[j]]]).refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_WM_LONG:
                  ((WeightedMFacetLong)mFacetLongs[_mArrayIndex[dynamicAR[j]]]).refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_WM_DOUBLE:
                  ((WeightedMFacetDouble)mFacetDoubles[_mArrayIndex[dynamicAR[j]]]).refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_WM_FLOAT:
                  ((WeightedMFacetFloat)mFacetFloats[_mArrayIndex[dynamicAR[j]]]).refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_WM_SHORT:
                  ((WeightedMFacetShort)mFacetShorts[_mArrayIndex[dynamicAR[j]]]).refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_WM_STRING:
                  ((WeightedMFacetString)mFacetStrings[_mArrayIndex[dynamicAR[j]]]).refresh(docID);
                  break;
                            
        default: 
                 break;
      }
    }
    
    return _cModel.score(shorts, ints, longs, floats, doubles, booleans, strings, sets, maps, mFacetInts, mFacetLongs, mFacetFloats, mFacetDoubles, mFacetShorts, mFacetStrings);
  }


  @Override
  public float newScore(int docID)
  {
  
    //update the dynamic parameters only when we have to.
    for(int j=0; j < dynamicAR.length; j++)
    {
      
      // only when the parameter is inner score variable or facet variable, we need to update the score function input parameter arrays; 
      switch (_types[dynamicAR[j]]) {
        case RelevanceJSONConstants.TYPENUMBER_FACET_INT:  
                  ints[_arrayIndex[dynamicAR[j]]] = ((TermIntList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(docID));
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_LONG:
                  longs[_arrayIndex[dynamicAR[j]]] = ((TermLongList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(docID));
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_DOUBLE:  
                  doubles[_arrayIndex[dynamicAR[j]]] = ((TermDoubleList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(docID));
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_FLOAT: 
                  floats[_arrayIndex[dynamicAR[j]]] = ((TermFloatList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(docID));
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_SHORT: 
                  shorts[_arrayIndex[dynamicAR[j]]] = ((TermShortList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(docID));
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_STRING:
                  strings[_arrayIndex[dynamicAR[j]]] = ((TermStringList)_termLists[_facetIndex[dynamicAR[j]]]).get(_orderArrays[_facetIndex[dynamicAR[j]]].get(docID));
                  break;
                  
        // multi-facet below;
        case RelevanceJSONConstants.TYPENUMBER_FACET_M_INT:
                  mFacetInts[_mArrayIndex[dynamicAR[j]]].refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_M_LONG:
                  mFacetLongs[_mArrayIndex[dynamicAR[j]]].refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_M_DOUBLE:
                  mFacetDoubles[_mArrayIndex[dynamicAR[j]]].refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_M_FLOAT:
                  mFacetFloats[_mArrayIndex[dynamicAR[j]]].refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_M_SHORT:
                  mFacetShorts[_mArrayIndex[dynamicAR[j]]].refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_M_STRING:
                  mFacetStrings[_mArrayIndex[dynamicAR[j]]].refresh(docID);
                  break;

                  
        // weighted multi-facet below;
        case RelevanceJSONConstants.TYPENUMBER_FACET_WM_INT:
                  ((WeightedMFacetInt)mFacetInts[_mArrayIndex[dynamicAR[j]]]).refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_WM_LONG:
                  ((WeightedMFacetLong)mFacetLongs[_mArrayIndex[dynamicAR[j]]]).refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_WM_DOUBLE:
                  ((WeightedMFacetDouble)mFacetDoubles[_mArrayIndex[dynamicAR[j]]]).refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_WM_FLOAT:
                  ((WeightedMFacetFloat)mFacetFloats[_mArrayIndex[dynamicAR[j]]]).refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_WM_SHORT:
                  ((WeightedMFacetShort)mFacetShorts[_mArrayIndex[dynamicAR[j]]]).refresh(docID);
                  break;
        case RelevanceJSONConstants.TYPENUMBER_FACET_WM_STRING:
                  ((WeightedMFacetString)mFacetStrings[_mArrayIndex[dynamicAR[j]]]).refresh(docID);
                  break;
                            
        default: 
                 break;
      }
    }
    
    return _cModel.score(shorts, ints, longs, floats, doubles, booleans, strings, sets, maps, mFacetInts, mFacetLongs, mFacetFloats, mFacetDoubles, mFacetShorts, mFacetStrings);
  }

  @Override
  public String getExplainString(float innerScore, int doc)
  {
    return _dt.funcBody;
  }


  @Override
  public void initializeReader(BoboIndexReader reader, JSONObject jsonParams) throws IOException
  {
    initialRunningData(reader,_cModel, _dt);      
  }


  @Override
  public void initializeGlobal(JSONObject jsonValues) throws JSONException
  {
    CompilationHelper.initializeValues(jsonValues, _dt);
  }


  @Override
  public ScoreAugmentFunction getCopy()
  {
    return new RuntimeRelevanceFunction(this._cModel, this._dt);
  }


  @Override
  public boolean useInnerScore()
  {
    return this._dt.useInnerScore;
  }
  
  public static class RuntimeRelevanceFunctionFactory extends CustomRelevanceFunctionFactory{

    RuntimeRelevanceFunction _rrf;
    public RuntimeRelevanceFunctionFactory(RuntimeRelevanceFunction rrf)
    {
      _rrf = rrf;
    }
    
    @Override
    public CustomRelevanceFunction build()
    {
      return (CustomRelevanceFunction) _rrf.getCopy();
    }
    
  }
  
}
