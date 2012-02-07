package com.senseidb.search.query;

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.TermDoubleList;
import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermShortList;
import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.util.BigSegmentedArray;

public class RelevanceQuery extends AbstractScoreAdjuster
{
  
  /* JSON keywords*/
  
  // (1) json keys;
  public static final String           KW_VALUES               = "values";
  public static final String           KW_VARIABLES            = "variables";
  public static final String           KW_FACETS               = "facets";
  public static final String           KW_FUNC_PARAMETERS      = "function_params";
  public static final String           KW_FUNCTION             = "function";

  // (2) supported types in json:
  // set type: [set_int, set_float, set_string, set_double, set_long]
  public static final String           KW_TYPE_SET_INT         = "set_int";
  public static final String           KW_TYPE_SET_FLOAT       = "set_float";
  public static final String           KW_TYPE_SET_STRING      = "set_string";
  public static final String           KW_TYPE_SET_DOUBLE      = "set_double";
  public static final String           KW_TYPE_SET_LONG        = "set_long";

  // normal type: [int, double, float, long, bool, string]
  public static final String           KW_TYPE_INT             = "int";
  public static final String           KW_TYPE_FLOAT           = "float";
  public static final String           KW_TYPE_STRING          = "string";
  public static final String           KW_TYPE_DOUBLE          = "double";
  public static final String           KW_TYPE_LONG            = "long";
  public static final String           KW_TYPE_BOOL            = "bool";

  // facet type support: [double, float, int, long, short, string]
  public static final String           KW_TYPE_FACET_INT       = "int";
  public static final String           KW_TYPE_FACET_FLOAT     = "float";
  public static final String           KW_TYPE_FACET_STRING    = "string";
  public static final String           KW_TYPE_FACET_DOUBLE    = "double";
  public static final String           KW_TYPE_FACET_LONG      = "long";
  public static final String           KW_TYPE_FACET_SHORT     = "short";

  // constant type:
  public static final String           KW_INNER_SCORE          = "_INNER_SCORE";
  public static final String           KW_NOW                  = "_NOW";
  
  
  /* Type Strings; */
  
  // (1) inner score type;
  private final String                 TYPE_INNER_SCORE  = "INNER_SCORE";  //actually a float value;
  
  // (2) general types:
  private final String                 TYPE_INT          = "INT";
  private final String                 TYPE_LONG         = "LONG";
  private final String                 TYPE_DOUBLE       = "DOUBLE";
  private final String                 TYPE_FLOAT        = "FLOAT";
  private final String                 TYPE_BOOLEAN      = "BOOLEAN";
  private final String                 TYPE_STRING       = "STRING";

  // hashset container types:
  private final String                 TYPE_SET_INT          = "SET_INT";
  private final String                 TYPE_SET_LONG         = "SET_LONG";
  private final String                 TYPE_SET_DOUBLE       = "SET_DOUBLE";
  private final String                 TYPE_SET_FLOAT        = "SET_FLOAT";
  private final String                 TYPE_SET_STRING       = "SET_STRING";
  
  private final String                 TYPE_SET_HEAD         = "SET";

  // (3) facet types:
  private final String                 TYPE_FACET_INT    = "FACET_INT";
  private final String                 TYPE_FACET_LONG   = "FACET_LONG";
  private final String                 TYPE_FACET_DOUBLE = "FACET_DOUBLE";
  private final String                 TYPE_FACET_FLOAT  = "FACET_FLOAT";
  private final String                 TYPE_FACET_SHORT  = "FACET_SHORT";
  private final String                 TYPE_FACET_STRING = "FACET_STRING";
  
  private final String                 TYPE_FACET_HEAD   = "FACET";  
  
  
  /* Type Numbers */
  
  // (1) inner score type number;
  private final int                 TYPENUMBER_INNER_SCORE  = 0;
  
  // (2) general type numbers:
  private final int                 TYPENUMBER_INT          = 1;
  private final int                 TYPENUMBER_LONG         = 2;
  private final int                 TYPENUMBER_DOUBLE       = 3;
  private final int                 TYPENUMBER_FLOAT        = 4;
  private final int                 TYPENUMBER_BOOLEAN      = 5;
  private final int                 TYPENUMBER_STRING       = 6;  
  
  private final int                 TYPENUMBER_SET          = 7;
  
  // (3) facet type numbers;
  private final int                 TYPENUMBER_FACET_INT    = 10;
  private final int                 TYPENUMBER_FACET_LONG   = 11;
  private final int                 TYPENUMBER_FACET_DOUBLE = 12;
  private final int                 TYPENUMBER_FACET_FLOAT  = 13;
  private final int                 TYPENUMBER_FACET_SHORT  = 14;
  private final int                 TYPENUMBER_FACET_STRING = 15;
  
  
  
  private static final long serialVersionUID = 1L;
  
  private static Logger logger = Logger.getLogger(RelevanceQuery.class);
  
  protected final Query _query;
  
  private HashMap<String, Object> hm_var = new HashMap<String, Object>();
  private HashMap<String, String> hm_type = new HashMap<String, String>();
  private HashMap<String, String> hm_symbol_facet = new HashMap<String, String>();
  private HashMap<String, Integer> hm_facet_index = new HashMap<String, Integer>();
  private LinkedList<String> lls_params = new LinkedList<String>();
  private String funcBody = null;
  private String classIDString = null;
  private CustomScorer cscorer = null;
  private int facetIndex = 0;
  
  
  static ClassPool pool = ClassPool.getDefault();
  static
  {
    pool.importPackage("java.util");

    pool.importPackage("it.unimi.dsi.fastutil.ints.IntOpenHashSet");
    pool.importPackage("it.unimi.dsi.fastutil.longs.LongOpenHashSet");
    pool.importPackage("it.unimi.dsi.fastutil.shorts.ShortOpenHashSet");
    pool.importPackage("it.unimi.dsi.fastutil.booleans.BooleanOpenHashSet");
    pool.importPackage("it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet");
    pool.importPackage("it.unimi.dsi.fastutil.floats.FloatOpenHashSet");
    pool.importPackage("it.unimi.dsi.fastutil.objects.ObjectOpenHashSet");
  }
  


  //white list of safe classes;
  static HashSet<String>    hs_safe = new HashSet<String>();      
  static
  {
    hs_safe.add("it.unimi.dsi.fastutil.ints.IntOpenHashSet");
    hs_safe.add("it.unimi.dsi.fastutil.longs.LongOpenHashSet");
    hs_safe.add("it.unimi.dsi.fastutil.shorts.ShortOpenHashSet");
    hs_safe.add("it.unimi.dsi.fastutil.booleans.BooleanOpenHashSet");
    hs_safe.add("it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet");
    hs_safe.add("it.unimi.dsi.fastutil.floats.FloatOpenHashSet");
    hs_safe.add("it.unimi.dsi.fastutil.objects.ObjectOpenHashSet");
    
    hs_safe.add("com.senseidb.search.query.RelevanceQuery");
    hs_safe.add("com.senseidb.search.query.CustomScorer");
    hs_safe.add("com.senseidb.search.query.RelevanceQuery$CustomLoader");
    
    hs_safe.add("java.lang.Object");
  }
  
  
  static HashMap<String, CustomScorer> hmModels = new HashMap<String, CustomScorer>();
  
//  "relevance":{
//               
//                  "variables": {
//                                 "set_int":["c","d"],  // supported hashset types: [set_int, set_float, set_string, set_double, set_long]
//                                 "int":["e","f"],       // supported normal variables: [int, double, float, long, bool, string]
//                                 "long":["g","h"]
//                                },
//                  "facets":{
//                               "int":["year","age"],   // facet type support: [double, float, int, long, short, string];
//                               "long":["time"]         // facet variable has the same name as the facet name, and they are defined inside this json;
//                            },
//                  
//                  "values":{
//                              "c":[1996,1997],
//                              "e":0.98
//                            },
//         
//                   // (2) scoring function and function input parameters in Java;
//                   //     A scoring function and its parameters are the model. A model changes when the function body or signature changes;
//                   
//                  //  params for the function. Symbol order matters, and symbols must be those defined above. innerScore MUST be used, otherwise, makes no sense to use the custom relevance;
//                  //  reserved keyword for internal parameters are:  "_INNER_SCORE" and "_NOW"     
//
//                   "function_params":["_INNER_SCORE", "timeVal", "_timeWeight", "_waterworldWeight", "_half_time"],               
//
//                   // the value string in the following JSONObject is like this (a return statement MUST appear as the last one):
//                         
//                      //    float delta = System.currentTimeMillis() - timeVal;
//                      //    float t = delta>0 ? delta : 0;
//                      //    float hour = t/(1000*3600);
//                      //    float timeScore = (float) Math.exp(-(hour/_half_time));
//                      //    float waterworldScore = _INNER_SCORE;
//                      //    float time = timeScore * _timeWeight;
//                      //    float water = waterworldScore  * _waterworldWeight;
//                      //    return  (time + water);
//                      
//                   "function":" A LONG JAVA CODE STRING HERE, ONLY AS FUNCTION BODY, NEEDS RETURN STATEMENT."
//            }
  
  
  
  
  /* A dummy testing relevance json:
   * 
   * 
            "relevance":{
                "variables":{
                     "set_int":["goodYear"],
                      "int":["thisYear"]
                    },
                "facets":{
                     "int":["year","mileage"],
                     "long":["groupid"]
                    },
               "values":{
                     "goodYear":[1996,1997],
                     "thisYear":1996
                   },
               "function_params":["_INNER_SCORE", "thisYear", "year"],              
               "function":" if(year==thisYear) return 3f  ; return  _INNER_SCORE  ;"
            }
   * 
   * */
  
  public RelevanceQuery(Query query, JSONObject relevance) throws JSONException
  {
    super(query);
    _query = query;
    preprocess(relevance);
  }
 



  private void preprocess(JSONObject relevance) throws JSONException
  {
    JSONObject jsonVariables = relevance.optJSONObject(KW_VARIABLES);
    JSONObject jsonFacets = relevance.optJSONObject(KW_FACETS);
    JSONObject jsonValues = relevance.optJSONObject(KW_VALUES);  // the json containing the values, could be null;
    
    
    //process the function body and parameters firstly;
    
    JSONArray jsonFuncParameter = relevance.optJSONArray(KW_FUNC_PARAMETERS);
    for(int j=0; j<jsonFuncParameter.length(); j++)
    {
      String paramName = jsonFuncParameter.optString(j);
      lls_params.add(paramName);
    }
    
    funcBody = relevance.optString(KW_FUNCTION);
    
    //process facet variables;
    Iterator<String> it_facet = jsonFacets.keys();
    while(it_facet.hasNext())
    {
      String facetType = it_facet.next();
      JSONArray facetArray = jsonFacets.getJSONArray(facetType);
      handleFacetSymbols(facetType, facetArray);
    }
    
    //process other variables;
    Iterator<String> it_var = jsonVariables.keys();
    while(it_var.hasNext())
    {
      String type = it_var.next();
      JSONArray varArray = jsonVariables.getJSONArray(type);
      
      //process set variable;
      if(KW_TYPE_SET_INT.equals(type) || KW_TYPE_SET_DOUBLE.equals(type) || KW_TYPE_SET_LONG.equals(type) || KW_TYPE_SET_FLOAT.equals(type) || KW_TYPE_SET_STRING.equals(type))
      {
        JSONArray sets = jsonVariables.getJSONArray(type);
        for(int i=0; i<sets.length(); i++)
        {
          String symbol = sets.getString(i);
          Set hs = null;
          JSONArray values = jsonValues.optJSONArray(symbol);
          
          if(values == null)
            throw new JSONException("variable "+ symbol + " does not have value.");
          
          for (int k =0; k < values.length(); k++){
            if(KW_TYPE_SET_INT.equals(type))
            {
              if(hs == null)
                hs = new IntOpenHashSet();
              hs.add(values.getInt(k));
            }
            else if (KW_TYPE_SET_DOUBLE.equals(type))
            {
              if(hs == null)
                hs = new DoubleOpenHashSet();
              hs.add(values.getDouble(k));
            }
            else if (KW_TYPE_SET_FLOAT.equals(type))
            {
              if(hs == null)
                hs = new FloatOpenHashSet();
              hs.add((float)values.getDouble(k));
            }
            else if (KW_TYPE_SET_LONG.equals(type))
            {
              if(hs == null)
                hs = new LongOpenHashSet();
              hs.add(values.getLong(k));
            }
            else if (KW_TYPE_SET_STRING.equals(type))
            {
              if(hs == null)
                hs = new ObjectOpenHashSet();
              hs.add(values.getString(k));
            }
          }
          if(hm_var.containsKey(symbol))
            throw new JSONException("Symbol "+ symbol + " already defined." );
          
          hm_var.put(symbol, hs);
          
          if(KW_TYPE_SET_INT.equals(type))
          {
            hm_type.put(symbol, TYPE_SET_INT);
          }
          else if (KW_TYPE_SET_DOUBLE.equals(type))
          {
            hm_type.put(symbol, TYPE_SET_DOUBLE);
          }
          else if (KW_TYPE_SET_FLOAT.equals(type))
          {
            hm_type.put(symbol, TYPE_SET_FLOAT);
          }
          else if (KW_TYPE_SET_LONG.equals(type))
          {
            hm_type.put(symbol, TYPE_SET_LONG);
          }
          else if (KW_TYPE_SET_STRING.equals(type))
          {
            hm_type.put(symbol, TYPE_SET_STRING);
          }
          
        }
      } // end of set variable;
      
      // process normal variable;
      // int, string, double, long
      else if(KW_TYPE_INT.equals(type) || KW_TYPE_DOUBLE.equals(type) || KW_TYPE_LONG.equals(type) || KW_TYPE_FLOAT.equals(type) || KW_TYPE_STRING.equals(type) || KW_TYPE_BOOL.equals(type))
      {
        JSONArray sets = jsonVariables.getJSONArray(type);
        
        for(int i=0; i< sets.length(); i++)
        {
        
          String symbol = sets.getString(i);

          if(hm_var.containsKey(symbol))
            throw new JSONException("Symbol "+ symbol + " already defined." );
          
          if( ! jsonValues.has(symbol))
            throw new JSONException("Symbol " + symbol + " was not assigned with a value." );
          
          if(KW_TYPE_INT.equals(type))
          {
            hm_var.put(symbol, jsonValues.getInt(symbol));
            hm_type.put(symbol, TYPE_INT);
          }
          else if (KW_TYPE_DOUBLE.equals(type))
          {
            hm_var.put(symbol, jsonValues.getDouble(symbol));
            hm_type.put(symbol, TYPE_DOUBLE);
          }
          else if (KW_TYPE_FLOAT.equals(type))
          {
            hm_var.put(symbol, ((float)jsonValues.getDouble(symbol)));
            hm_type.put(symbol, TYPE_FLOAT);
          }
          else if (KW_TYPE_LONG.equals(type))
          {
            hm_var.put(symbol, jsonValues.getLong(symbol));
            hm_type.put(symbol, TYPE_LONG);
          }
          else if (KW_TYPE_STRING.equals(type))
          {
            hm_var.put(symbol, jsonValues.getString(symbol));
            hm_type.put(symbol, TYPE_STRING);
          }
          else if(KW_TYPE_BOOL.equals(type))
          {
            hm_var.put(symbol, jsonValues.getBoolean(symbol));
            hm_type.put(symbol, TYPE_STRING);
          }
        }
      }
    }// end of normal variable while;

    // add now variable;
    String symbolNow = KW_NOW;
    long now = System.currentTimeMillis();
    hm_var.put(symbolNow, now);
    hm_type.put(symbolNow, TYPE_LONG);
    
    // add innerscore;
    String symbolInnerScore = KW_INNER_SCORE; 
    hm_var.put(symbolInnerScore, symbolInnerScore);
    hm_type.put(symbolInnerScore, TYPE_INNER_SCORE);
    
    
    
    
    if(funcBody == null || funcBody.length()==0)
      throw new JSONException("No function body found.");
    
    if(funcBody.indexOf("return ")==-1)
      throw new JSONException("No return statement in the function body.");
    

    //check if all the parameters have defined;
    for(int i=0; i< lls_params.size(); i++)
    {
      String symbol = lls_params.get(i);
      if( !hm_type.containsKey(symbol))
        throw new JSONException("function parameter: " + symbol + " was not defined.");
      
      String type = hm_type.get(symbol);
      if(type.startsWith(TYPE_FACET_HEAD))
      {
        if( !hm_symbol_facet.containsKey(symbol))
          throw new JSONException("function parameter: " + symbol + " was not defined.");
      }
      else
      {
        if(!hm_var.containsKey(symbol))
          throw new JSONException("function parameter: " + symbol + " was not defined.");
      }
    }

    lls_params = filterParameters(lls_params, funcBody);
    
    String paramString = getParamString(lls_params);
    
    classIDString = funcBody + paramString;
    String className = "CRel"+ classIDString.hashCode();
    logger.info("Custom relevance class name is:"+ className);
    

    if(hmModels.containsKey(className))
      cscorer = hmModels.get(className);
    else
    {

      synchronized(this)
      {
        
        if(hmModels.containsKey(className))
        {
          cscorer = hmModels.get(className);
          return;
        }
        
        
        CtClass ch = pool.makeClass(className);
        
        CtClass ci;
        try
        {
          ci = pool.get("com.senseidb.search.query.CustomScorer");
        }
        catch (NotFoundException e)
        {
          logger.info(e.getMessage());
          throw new JSONException(e);
        }
        ch.addInterface(ci);
        String functionString = makeFuncString(funcBody, hm_type, lls_params);
        
        
        CtMethod m;
        try
        {
          m = CtNewMethod.make(functionString, ch);
        }
        catch (CannotCompileException e)
        {
          logger.info(e.getMessage());
          throw new JSONException(e);
        }
        
        try
        {
          ch.addMethod(m);
        }
        catch (CannotCompileException e)
        {
          logger.info(e.getMessage());
          throw new JSONException(e);
        }
        
        Class h;
        try
        {
//          h = pool.toClass(ch, RelevanceQuery.class.getClassLoader());
          h = pool.toClass(ch, new CustomLoader(RelevanceQuery.class.getClassLoader(), className));
        }
        catch (CannotCompileException e)
        {
          if(hmModels.containsKey(className))
          {
            cscorer = hmModels.get(className);
            return;
          }
          else
          {
            logger.info(e.getMessage());
            throw new JSONException(e);
          }
        }
        
        try
        {
          cscorer = (CustomScorer)h.newInstance();
        }
        catch (InstantiationException e)
        {
          logger.info(e.getMessage());
          throw new JSONException(e);
        }
        catch (IllegalAccessException e)
        {
          logger.info(e.getMessage());
          throw new JSONException(e);
        }
        
        hmModels.put(className, cscorer);
      }        
    }
    
  }
  
  





  private String getParamString(LinkedList<String> lls_params)
  {
    StringBuilder sb = new StringBuilder();
    for(String param : lls_params)
    {
      sb.append(param);
      sb.append("#");
    }
    return sb.toString();
  }


  private LinkedList<String> filterParameters(LinkedList<String> lls_params, String funcBody)
  {
    LinkedList<String> lls_new = new LinkedList<String>();
    for(String param : lls_params)
    {
      if(  !(funcBody.indexOf(param) == -1))
        lls_new.add(param);
    }
    return lls_new;
  }

  
  private void handleFacetSymbols(String facetType, JSONArray facetArray) throws JSONException
  {
    String type = null;
    
    if("int".equals(facetType)) 
      type = TYPE_FACET_INT;
    else if("short".equals(facetType)) 
      type = TYPE_FACET_SHORT;
    else if("double".equals(facetType))
      type = TYPE_FACET_DOUBLE;
    else if("float".equals(facetType))
      type = TYPE_FACET_FLOAT;
    else if("long".equals(facetType))
      type = TYPE_FACET_LONG;
    else if("string".equals(facetType))
      type = TYPE_FACET_STRING;
    
    if(type == null)
      throw new JSONException("wrong facet type in facet variable definition json");
    
    for(int i=0; i< facetArray.length(); i++)
    {
      String facetName = facetArray.getString(i);
      String symbol = facetName;

      if(hm_symbol_facet.containsKey(symbol))
        throw new JSONException("facet Symbol "+ symbol + " already defined." );

      if(hm_facet_index.containsKey(facetName))
        throw new JSONException("facet name "+ facetName + " already assigned to a symbol." );
      
      hm_symbol_facet.put(symbol, facetName);
      hm_facet_index.put(facetName, facetIndex);
      facetIndex++;
      hm_type.put(symbol, type);
    }
    
  }

  private String makeFuncString(String funcBody, 
                                HashMap<String, String> hm_type,
                                LinkedList<String> lls_params) throws JSONException
  {
//    "public float score(Object[] objs) {  Integer inta = (Integer)objs[0]; System.out.println(inta);  HashMap hm = (HashMap)objs[2]; System.out.println(hm.get(\"good\")); return b; }"
    
    StringBuffer sb = new StringBuffer();
    sb.append("public float score(short[] shorts, int[] ints, long[] longs, float[] floats, double[] doubles, boolean[] booleans, String[] strings, Set[] sets) {");
    
    int short_index = 0;
    int int_index = 0;
    int long_index = 0;
    int float_index = 0;
    int double_index = 0;
    int boolean_index = 0;
    int string_index = 0;
    int set_index = 0;
    
    for(int i=0; i< lls_params.size();i++)
    {
      String paramName = lls_params.get(i);
      
      if(!hm_type.containsKey(paramName))
        throw new JSONException("function arameter " + paramName + " is not defined.");
      
      if(hm_type.get(paramName).equals(TYPE_INT) || hm_type.get(paramName).equals(TYPE_FACET_INT))
      {
        sb.append(" int " + paramName + " = ints[" + int_index + "]; ");
        int_index++;
      }
      else if(hm_type.get(paramName).equals(TYPE_LONG) || hm_type.get(paramName).equals(TYPE_FACET_LONG))
      {
        sb.append(" long " + paramName + " = longs[" + long_index +"];  ");
        long_index++;
      }
      else if(hm_type.get(paramName).equals(TYPE_DOUBLE) || hm_type.get(paramName).equals(TYPE_FACET_DOUBLE))
      {
        sb.append(" double " + paramName + " = doubles["+ double_index +"]; ");
        double_index++;
      }
      else if(hm_type.get(paramName).equals(TYPE_FLOAT) || hm_type.get(paramName).equals(TYPE_FACET_FLOAT))
      {
        sb.append(" float " + paramName + " = floats["+ float_index +"]; ");
        float_index++;
      }      
      else if(hm_type.get(paramName).equals(TYPE_STRING) || hm_type.get(paramName).equals(TYPE_FACET_STRING))
      {
        sb.append(" String " + paramName + " = strings["+  string_index +"]; ");
        string_index++;
      }
      else if(hm_type.get(paramName).equals(TYPE_BOOLEAN))
      {
        sb.append(" boolean " + paramName + " = booleans["+ boolean_index +"]; ");
        boolean_index++;
      }
      else if(hm_type.get(paramName).equals(TYPE_FACET_SHORT))
      {
        sb.append(" short " + paramName + " = shorts["+ short_index +"]; ");
        short_index++;
      }
      else if(hm_type.get(paramName).equals(TYPE_SET_INT))
      {
        sb.append(" it.unimi.dsi.fastutil.ints.IntOpenHashSet " + paramName + " = (it.unimi.dsi.fastutil.ints.IntOpenHashSet) sets["+ set_index +"]; ");
        set_index++;
      }
      else if(hm_type.get(paramName).equals(TYPE_SET_LONG))
      {
        sb.append(" it.unimi.dsi.fastutil.longs.LongOpenHashSet " + paramName + " = (it.unimi.dsi.fastutil.longs.LongOpenHashSet) sets["+ set_index +"]; ");
        set_index++;
      }
      else if(hm_type.get(paramName).equals(TYPE_SET_DOUBLE))
      {
        sb.append(" it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet " + paramName + " = (it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet) sets["+ set_index +"]; ");
        set_index++;
      }
      else if(hm_type.get(paramName).equals(TYPE_SET_FLOAT))
      {
        sb.append(" it.unimi.dsi.fastutil.floats.FloatOpenHashSet " + paramName + " = (it.unimi.dsi.fastutil.floats.FloatOpenHashSet) sets["+ set_index +"]; ");
        set_index++;
      }
      else if(hm_type.get(paramName).equals(TYPE_SET_STRING))
      {
        sb.append(" it.unimi.dsi.fastutil.objects.ObjectOpenHashSet " + paramName + " = (it.unimi.dsi.fastutil.objects.ObjectOpenHashSet) sets["+ set_index +"]; ");
        set_index++;
      }
      else if(hm_type.get(paramName).equals(TYPE_INNER_SCORE))
      {
        sb.append(" float " + paramName + " = floats["+ float_index +"]; ");
        float_index++;
      }
    }
    
    sb.append(funcBody);
    sb.append("}");
    return sb.toString();
  }



  @Override
  protected Scorer createScorer(final Scorer innerScorer,
                                IndexReader reader,
                                boolean scoreDocsInOrder,
                                boolean topScorer) throws IOException
  {
    if(cscorer == null)
      return innerScorer;
    
    if (reader instanceof BoboIndexReader ){
      BoboIndexReader boboReader = (BoboIndexReader)reader;
      
      int numFacet = hm_symbol_facet.keySet().size();
      final BigSegmentedArray[] orderArrays = new BigSegmentedArray[numFacet];
      final TermValueList[] termLists = new TermValueList[numFacet];
      
      Iterator<String> iter_facet = hm_facet_index.keySet().iterator();
      while(iter_facet.hasNext()){
        String facetName = iter_facet.next();
        
        // validation;
        Object dataObj = boboReader.getFacetData(facetName);
        if ( ! (dataObj instanceof FacetDataCache<?>))
          return innerScorer;
        
        int index = hm_facet_index.get(facetName);
        orderArrays[index] = ((FacetDataCache)(boboReader.getFacetData(facetName))).orderArray;
        termLists[index] = ((FacetDataCache)(boboReader.getFacetData(facetName))).valArray;
      }
      
      final int paramSize = lls_params.size();
      
      final int[] types = new int[paramSize];  //store each parameter's type;
      final int[] facetIndex = new int[paramSize];  // if this parameter is a facet, what is its index number in the facet data array;
      final int[] arrayIndex = new int[paramSize];  // for each paramter, what is its index number in its own parameter array when passing into the function;
      
      updateArrayIndex(paramSize, types, facetIndex, arrayIndex);

      return new CodeGenScorer(innerScorer, cscorer, orderArrays, termLists, types, facetIndex, arrayIndex, paramSize);
    }
    else{
      return innerScorer;
    }
  }

  
  private void updateArrayIndex(int paramSize, int[] types, int[] facetIndex, int[] arrayIndex)
  {
    int short_index = 0;
    int int_index = 0;
    int long_index = 0;
    int float_index = 0;
    int double_index = 0;
    int boolean_index = 0;
    int string_index = 0;
    int set_index = 0;

    for(int i=0; i< paramSize; i++)
    {
      if(hm_type.get(lls_params.get(i)).equals(TYPE_INNER_SCORE)){
        types[i] = TYPENUMBER_INNER_SCORE;  //inner_score type parameter;
        facetIndex[i] = -1;  //should not be used;
        arrayIndex[i] = float_index;
        float_index++;
      }
      else if (hm_type.get(lls_params.get(i)).startsWith(TYPE_FACET_HEAD))
      {
        String type = hm_type.get(lls_params.get(i));
        
        if(type.equals(TYPE_FACET_INT))
        {
          types[i] = TYPENUMBER_FACET_INT;
          arrayIndex[i] = int_index;
          int_index++;
        }
        else if (type.equals(TYPE_FACET_LONG))
        {
          types[i] = TYPENUMBER_FACET_LONG;
          arrayIndex[i] = long_index;
          long_index++;
        }
        else if (type.equals(TYPE_FACET_DOUBLE))
        {
          types[i] = TYPENUMBER_FACET_DOUBLE;
          arrayIndex[i] = double_index;
          double_index++;
        }
        else if (type.equals(TYPE_FACET_FLOAT))
        {
          types[i] = TYPENUMBER_FACET_FLOAT;
          arrayIndex[i] = float_index;
          float_index++;
        }
        else if (type.equals(TYPE_FACET_SHORT))
        {
          types[i] = TYPENUMBER_FACET_SHORT;
          arrayIndex[i] = short_index;
          short_index++;
        }
        else if (type.equals(TYPE_FACET_STRING))
        {
          types[i] = TYPENUMBER_FACET_STRING;
          arrayIndex[i] = string_index;
          string_index++;
        }
        
        String facetName = hm_symbol_facet.get(lls_params.get(i));
        int index = hm_facet_index.get(facetName);
        facetIndex[i] = index;  // record the facet index;
      }
      else
      {
        String type = hm_type.get(lls_params.get(i));  //normal type parameter;
        
        if(type.equals(TYPE_INT))
        {
          types[i] = TYPENUMBER_INT;
          arrayIndex[i] = int_index;
          int_index++;
        }
        else if (type.equals(TYPE_LONG))
        {
          types[i] = TYPENUMBER_LONG;
          arrayIndex[i] = long_index;
          long_index++;
        }
        else if (type.equals(TYPE_DOUBLE))
        {
          types[i] = TYPENUMBER_DOUBLE;
          arrayIndex[i] = double_index;
          double_index++;
        }
        else if (type.equals(TYPE_FLOAT))
        {
          types[i] = TYPENUMBER_FLOAT;
          arrayIndex[i] = float_index;
          float_index++;
        }
        else if (type.equals(TYPE_BOOLEAN))
        {
          types[i] = TYPENUMBER_BOOLEAN;
          arrayIndex[i] = boolean_index;
          boolean_index++;
        }
        else if (type.equals(TYPE_STRING))
        {
          types[i] = TYPENUMBER_STRING;
          arrayIndex[i] = string_index;
          string_index++;
        }
        else if (type.startsWith(TYPE_SET_HEAD))
        {
          types[i] = TYPENUMBER_SET;
          arrayIndex[i] = set_index;
          set_index++;
        }
        
        facetIndex[i] = -1;  // should not be used;
      }
    }    
  }


  public class  CodeGenScorer extends Scorer{

    final Scorer _innerScorer;
    final CustomScorer _cscorer;
    
    final BigSegmentedArray[] _orderArrays;
    final TermValueList[] _termLists;
    
    final int[] _types;
    final int[] _facetIndex;
    final int[] _arrayIndex;
    
    final int _paramSize;
    
//    final Object[] _objs;
    
    final short[] shorts;
    final int[] ints;
    final long[] longs;
    final float[] floats;
    final double[] doubles;
    final boolean[] booleans;
    final String[] strings;
    final Set[] sets;
    
    final int[] dynamicAR;
    
    public CodeGenScorer(Scorer innerScorer, 
                         CustomScorer cscorer, 
                         BigSegmentedArray[] orderArrays,
                         TermValueList[] termLists,
                         int[] types,
                         int[] facetIndex,
                         int[] arrayIndex,
                         int paramSize
                         )
    {
      super(innerScorer.getSimilarity());
      
      _innerScorer = innerScorer;
      _cscorer = cscorer;
      _orderArrays = orderArrays;
      _termLists = termLists;
      _types = types;
      _facetIndex = facetIndex;
      _arrayIndex = arrayIndex;
      _paramSize = paramSize;
      
      shorts    = new short[_paramSize];
      ints      = new int[_paramSize];
      longs     = new long[_paramSize];
      floats    = new float[_paramSize];
      doubles   = new double[_paramSize];
      booleans  = new boolean[_paramSize];
      strings   = new String[_paramSize];
      sets      = new Set[_paramSize];
      
      ArrayList<Integer> arDynamic = new ArrayList<Integer>();
      
      // prepare the static variable;
      for(int i=0; i<_paramSize; i++)
      {
        switch (_types[i]) {
          case TYPENUMBER_INT:  
                    ints[_arrayIndex[i]] = ((Integer)hm_var.get(lls_params.get(i))).intValue();
                    break;
          case TYPENUMBER_LONG:
                    longs[_arrayIndex[i]] = ((Long)hm_var.get(lls_params.get(i))).longValue();
                    break;
          case TYPENUMBER_DOUBLE:  
                    doubles[_arrayIndex[i]] = ((Double)hm_var.get(lls_params.get(i))).doubleValue();
                    break;
          case TYPENUMBER_FLOAT: 
                    floats[_arrayIndex[i]] = ((Float)hm_var.get(lls_params.get(i))).floatValue();
                    break;
          case TYPENUMBER_BOOLEAN: 
                    booleans[_arrayIndex[i]] = ((Boolean)hm_var.get(lls_params.get(i))).booleanValue();
                    break;
          case TYPENUMBER_STRING:
                    strings[_arrayIndex[i]] = (String) hm_var.get(lls_params.get(i));
                    break;
          case TYPENUMBER_SET:
                    sets[_arrayIndex[i]] = (Set)hm_var.get(lls_params.get(i));
                    break;
          default: 
                    arDynamic.add(i);
        }
      }
      
      dynamicAR = convertIntegers(arDynamic);
      
    }
    
    public int[] convertIntegers(List<Integer> integers)
    {
        int[] ret = new int[integers.size()];
        Iterator<Integer> iterator = integers.iterator();
        for (int i = 0; i < ret.length; i++)
        {
            ret[i] = iterator.next().intValue();
        }
        return ret;
    }
    
    @Override
    public float score() throws IOException {
      
      //update the dynamic parameters only when we have to.
      for(int j=0; j < dynamicAR.length; j++)
      {
        
        // only when the parameter is inner score variable or facet variable, we need to update the score function input parameter arrays; 
        switch (_types[dynamicAR[j]]) {
          case TYPENUMBER_INNER_SCORE:  
                    floats[_arrayIndex[dynamicAR[j]]] = _innerScorer.score();
                    break;
          case TYPENUMBER_FACET_INT:  
                    ints[_arrayIndex[dynamicAR[j]]] = ((TermIntList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(_innerScorer.docID()));
                    break;
          case TYPENUMBER_FACET_LONG:
                    longs[_arrayIndex[dynamicAR[j]]] = ((TermLongList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(_innerScorer.docID()));
                    break;
          case TYPENUMBER_FACET_DOUBLE:  
                    doubles[_arrayIndex[dynamicAR[j]]] = ((TermDoubleList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(_innerScorer.docID()));
                    break;
          case TYPENUMBER_FACET_FLOAT: 
                    floats[_arrayIndex[dynamicAR[j]]] = ((TermFloatList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(_innerScorer.docID()));
                    break;
          case TYPENUMBER_FACET_SHORT: 
                    shorts[_arrayIndex[dynamicAR[j]]] = ((TermShortList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(_innerScorer.docID()));
                    break;
          case TYPENUMBER_FACET_STRING:
                    strings[_arrayIndex[dynamicAR[j]]] = ((TermStringList)_termLists[_facetIndex[dynamicAR[j]]]).get(_orderArrays[_facetIndex[dynamicAR[j]]].get(_innerScorer.docID()));
                    break;
          default: 
                   break;
        }
      }
      
//      float score(short[] shorts, int[] ints, long[] longs, float[] floats, double[] doubles, boolean[] booleans, String[] strings, Set[] sets);
      return _cscorer.score(shorts, ints, longs, floats, doubles, booleans, strings, sets);
    }

    @Override
    public int advance(int target) throws IOException {
      return _innerScorer.advance(target);
    }

    @Override
    public int docID() {
      return _innerScorer.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      return _innerScorer.nextDoc();
    }
    
    
  }



  @Override
  protected Explanation createExplain(Explanation innerExplain,
                                      IndexReader reader,
                                      int doc)
  {
    if(cscorer == null)
      return createDummyExplain(innerExplain, "cscorer is null, return innerExplanation.");
    
    if (reader instanceof BoboIndexReader ){
      BoboIndexReader boboReader = (BoboIndexReader)reader;
      
      int numFacet = hm_symbol_facet.keySet().size();
      final BigSegmentedArray[] orderArrays = new BigSegmentedArray[numFacet];
      final TermValueList[] termLists = new TermValueList[numFacet];
      
      Iterator<String> iter_facet = hm_facet_index.keySet().iterator();
      while(iter_facet.hasNext()){
        String facetName = iter_facet.next();
        
        // validation;
        Object dataObj = boboReader.getFacetData(facetName);
        if ( ! (dataObj instanceof FacetDataCache<?>))
          return createDummyExplain(innerExplain, "Facet does not exist, return innerExplanation.");
        
        int index = hm_facet_index.get(facetName);
        orderArrays[index] = ((FacetDataCache)(boboReader.getFacetData(facetName))).orderArray;
        termLists[index] = ((FacetDataCache)(boboReader.getFacetData(facetName))).valArray;
      }
      
      Explanation finalExpl = new Explanation();
      finalExpl.addDetail(innerExplain);
      
      final int paramSize = lls_params.size();
      
      final int[] types = new int[paramSize];
      final int[] facetIndex = new int[paramSize];
      final int[] arrayIndex = new int[paramSize];
      
      updateArrayIndex(paramSize, types, facetIndex, arrayIndex);
      
      short[] shorts = new short[paramSize];
      int[] ints = new int[paramSize];
      long[] longs = new long[paramSize];
      float[] floats = new float[paramSize];
      double[] doubles = new double[paramSize];
      boolean[] booleans = new boolean[paramSize];
      String[] strings = new String[paramSize];
      Set[] sets = new Set[paramSize];
      
      for(int i=0; i<paramSize; i++)
      {
        switch (types[i]) {
        case TYPENUMBER_INT:  
                  ints[arrayIndex[i]] = ((Integer)hm_var.get(lls_params.get(i))).intValue();
                  break;
        case TYPENUMBER_LONG:
                  longs[arrayIndex[i]] = ((Long)hm_var.get(lls_params.get(i))).longValue();
                  break;
        case TYPENUMBER_DOUBLE:  
                  doubles[arrayIndex[i]] = ((Double)hm_var.get(lls_params.get(i))).doubleValue();
                  break;
        case TYPENUMBER_FLOAT: 
                  floats[arrayIndex[i]] = ((Float)hm_var.get(lls_params.get(i))).floatValue();
                  break;
        case TYPENUMBER_BOOLEAN: 
                  booleans[arrayIndex[i]] = ((Boolean)hm_var.get(lls_params.get(i))).booleanValue();
                  break;
        case TYPENUMBER_STRING:
                  strings[arrayIndex[i]] = (String) hm_var.get(lls_params.get(i));
                  break;
        case TYPENUMBER_SET:
                  sets[arrayIndex[i]] = (Set)hm_var.get(lls_params.get(i));
                  break;
                  
        case TYPENUMBER_INNER_SCORE:  
                  floats[arrayIndex[i]] = innerExplain.getValue();
                  break;
        case TYPENUMBER_FACET_INT:  
                  ints[arrayIndex[i]] = ((TermIntList)termLists[facetIndex[i]]).getPrimitiveValue(orderArrays[facetIndex[i]].get(doc));
                  break;
        case TYPENUMBER_FACET_LONG:
                  longs[arrayIndex[i]] = ((TermLongList)termLists[facetIndex[i]]).getPrimitiveValue(orderArrays[facetIndex[i]].get(doc));
                  break;
        case TYPENUMBER_FACET_DOUBLE:  
                  doubles[arrayIndex[i]] = ((TermDoubleList)termLists[facetIndex[i]]).getPrimitiveValue(orderArrays[facetIndex[i]].get(doc));
                  break;
        case TYPENUMBER_FACET_FLOAT: 
                  floats[arrayIndex[i]] = ((TermFloatList)termLists[facetIndex[i]]).getPrimitiveValue(orderArrays[facetIndex[i]].get(doc));
                  break;
        case TYPENUMBER_FACET_SHORT: 
                  shorts[arrayIndex[i]] = ((TermShortList)termLists[facetIndex[i]]).getPrimitiveValue(orderArrays[facetIndex[i]].get(doc));
                  break;
        case TYPENUMBER_FACET_STRING:
                  strings[arrayIndex[i]] = ((TermStringList)termLists[facetIndex[i]]).get(orderArrays[facetIndex[i]].get(doc));
                  break;
        default: 
                 break;
        }
      }
      
      float value = cscorer.score(shorts, ints, longs, floats, doubles, booleans, strings, sets);
      finalExpl.setValue(value);
      finalExpl.setDescription("Custom score: "+ value + "  function:"+funcBody);
      return finalExpl;
    }
    else{
      return createDummyExplain(innerExplain, "Non-Bobo reader with custom scorer. Should not arrive here.");
    }
  }
  
  private Explanation createDummyExplain(Explanation innerExplain, String message)
  {
    Explanation finalExpl = new Explanation();
    finalExpl.addDetail(innerExplain);
    finalExpl.setDescription(message);
    finalExpl.setValue(innerExplain.getValue());
    return finalExpl;
  }

  
  class CustomLoader extends ClassLoader {

    private ClassLoader _cl;
    private String _target;
    
    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      
      if(hs_safe.contains(name) || name.equals(_target))
        return _cl.loadClass(name);
      else 
        throw new ClassNotFoundException();
    }
    
    public CustomLoader(ClassLoader cl, String target) {
        _cl = cl;
        _target = target;
    }
  }

}
