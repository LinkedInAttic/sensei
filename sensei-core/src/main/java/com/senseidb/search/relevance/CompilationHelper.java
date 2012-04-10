package com.senseidb.search.relevance;

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;

public class CompilationHelper
{

  private static Logger logger = Logger.getLogger(CompilationHelper.class);
  
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
    
    pool.importPackage("it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap");
    pool.importPackage("it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap");
    pool.importPackage("it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap");
    pool.importPackage("it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap");
    pool.importPackage("it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap");
    
    pool.importPackage("it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap");
    pool.importPackage("it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap");
    pool.importPackage("it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap");
    pool.importPackage("it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap");
    pool.importPackage("it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap");
    
    pool.importPackage("it.unimi.dsi.fastutil.objects.AbstractObject2FloatMap");
    
    pool.importPackage("com.senseidb.search.relevance.MFacet");
    pool.importPackage("com.senseidb.search.relevance.MFacetDouble");
    pool.importPackage("com.senseidb.search.relevance.MFacetFloat");
    pool.importPackage("com.senseidb.search.relevance.MFacetInt");
    pool.importPackage("com.senseidb.search.relevance.MFacetLong");
    pool.importPackage("com.senseidb.search.relevance.MFacetShort");
    pool.importPackage("com.senseidb.search.relevance.MFacetString");
    
    pool.importPackage("com.senseidb.search.relevance.WeightedMFacet");
    pool.importPackage("com.senseidb.search.relevance.WeightedMFacetDouble");
    pool.importPackage("com.senseidb.search.relevance.WeightedMFacetFloat");
    pool.importPackage("com.senseidb.search.relevance.WeightedMFacetInt");
    pool.importPackage("com.senseidb.search.relevance.WeightedMFacetLong");
    pool.importPackage("com.senseidb.search.relevance.WeightedMFacetShort");
    pool.importPackage("com.senseidb.search.relevance.WeightedMFacetString");
    
//    pool.appendClassPath( new LoaderClassPath(RelevanceQuery.class.getClassLoader()));
    pool.insertClassPath(new ClassClassPath(RelevanceQuery.class));
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
    
    hs_safe.add("it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap");
    hs_safe.add("it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap");
    hs_safe.add("it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap");
    hs_safe.add("it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap");
    hs_safe.add("it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap");
    
    hs_safe.add("it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap");
    hs_safe.add("it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap");
    hs_safe.add("it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap");
    hs_safe.add("it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap");
    hs_safe.add("it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap");
    
    hs_safe.add("it.unimi.dsi.fastutil.objects.AbstractObject2FloatMap");
    
    hs_safe.add("com.senseidb.search.relevance.MFacet");
    hs_safe.add("com.senseidb.search.relevance.MFacetDouble");
    hs_safe.add("com.senseidb.search.relevance.MFacetFloat");
    hs_safe.add("com.senseidb.search.relevance.MFacetInt");
    hs_safe.add("com.senseidb.search.relevance.MFacetLong");
    hs_safe.add("com.senseidb.search.relevance.MFacetShort");
    hs_safe.add("com.senseidb.search.relevance.MFacetString");
    
    hs_safe.add("com.senseidb.search.relevance.WeightedMFacet");
    hs_safe.add("com.senseidb.search.relevance.WeightedMFacetDouble");
    hs_safe.add("com.senseidb.search.relevance.WeightedMFacetFloat");
    hs_safe.add("com.senseidb.search.relevance.WeightedMFacetInt");
    hs_safe.add("com.senseidb.search.relevance.WeightedMFacetLong");
    hs_safe.add("com.senseidb.search.relevance.WeightedMFacetShort");
    hs_safe.add("com.senseidb.search.relevance.WeightedMFacetString");
    
    
    hs_safe.add("com.senseidb.search.relevance.RelevanceQuery");
    hs_safe.add("com.senseidb.search.relevance.CustomMathModel");
    hs_safe.add("com.senseidb.search.relevance.RelevanceQuery$CustomLoader");
    
    hs_safe.add("java.lang.Object");
    hs_safe.add("java.lang.Exception");
    hs_safe.add("java.lang.Boolean");
    hs_safe.add("java.lang.Byte");
    hs_safe.add("java.lang.Double");
    hs_safe.add("java.lang.Float");
    hs_safe.add("java.lang.Integer");
    hs_safe.add("java.lang.Long");
    hs_safe.add("java.lang.Math");
    hs_safe.add("java.lang.Number");
    hs_safe.add("java.lang.Short");
    hs_safe.add("java.lang.String");
    hs_safe.add("java.lang.StringBuffer");
    hs_safe.add("java.lang.StringBuilder");
    hs_safe.add("java.math.BigDecimal");
    hs_safe.add("java.math.BigInteger");
    hs_safe.add("java.math.MathContext");
    hs_safe.add("java.math.RoundingMode");
    
  }
  
  public static int MAX_NUM_MODELS  = 10000;
  static HashMap<String, CustomMathModel> hmModels = new HashMap<String, CustomMathModel>();
  
//"relevance":{
//
//            // (1) Model definition part; this json is used to define a model (input variables, columns/facets, and function parameters and body);    
//            "model":{
//           
//              "variables": {
//                             "set_int":["c","d"],  // supported hashset types: [set_int, set_float, set_string, set_double, set_long]
//                             "map_int_float":["j"],  // currently supported hashmap: [map_int_float, map_int_double, map_int_*...] [map_string_int, map_string_float, map_string_*]
//                             "int":["e","f"],       // supported normal variables: [int, double, float, long, bool, string]
//                             "long":["g","h"]
//                            },
//              "facets":{
//                           "int":["year","age"],   // facet type support: [double, float, int, long, short, string];
//                           "long":["time"]         // facet variable has the same name as the facet name, and they are defined inside this json;
//                        },
//              
//               // (2) scoring function and function input parameters in Java;
//               //     A scoring function and its parameters are the model. A model changes when the function body or signature changes;
//               
//              //  params for the function. Symbol order matters, and symbols must be those defined above. innerScore MUST be used, otherwise, makes no sense to use the custom relevance;
//              //  reserved keyword for internal parameters are:  "_INNER_SCORE" and "_NOW"     
//
//               "function_params":["_INNER_SCORE", "timeVal", "_timeWeight", "_waterworldWeight", "_half_time"],               
//
//               // the value string in the following JSONObject is like this (a return statement MUST appear as the last one):
//                     
//                  //    float delta = System.currentTimeMillis() - timeVal;
//                  //    float t = delta>0 ? delta : 0;
//                  //    float hour = t/(1000*3600);
//                  //    float timeScore = (float) Math.exp(-(hour/_half_time));
//                  //    float waterworldScore = _INNER_SCORE;
//                  //    float time = timeScore * _timeWeight;
//                  //    float water = waterworldScore  * _waterworldWeight;
//                  //    return  (time + water);
//                  
//               "function":" A LONG JAVA CODE STRING HERE, ONLY AS FUNCTION BODY, NEEDS RETURN STATEMENT."
//             },
//             
//             //(2) Input values for the model above, if the model requires input values;
//             "values":{
//               "c":[1996,1997],
//               "e":0.98,
//               "j":{"key":[1,2,3], "value":[2.3, 3.4, 2.9]}      // a user input hashmap;
//             }
//        }




/* A dummy testing relevance json inside a query request json:
* 
* 
{
    "query": {
        "query_string": {
            "query": "",
            "relevance":{
            
                "model":{
                    "variables":{
                         "set_int":["goodYear"],
                         "int":["thisYear"],
                         "string":["coolTag"],
                         "map_int_float":["mileageWeight"],
                         "map_int_string":["yearcolor"],
                         "map_string_float":["colorweight"],
                         "map_string_string":["categorycolor"]
                        },
                    "facets":{
                         "int":["year","mileage"],
                         "long":["groupid"],
                         "string":["color","category"],
                         "mstring":["tags"] 
                        },
                    "function_params":["_INNER_SCORE", "thisYear", "year","goodYear","mileageWeight","mileage","color", "yearcolor", "colorweight", "category", "categorycolor"],  
                    "function":"  if(tags.contains(coolTag)) return 999999f; if(categorycolor.containsKey(category) && categorycolor.get(category).equals(color))  return 10000f; if(colorweight.containsKey(color) ) return 200f + colorweight.getFloat(color); if(yearcolor.containsKey(year) && yearcolor.get(year).equals(color)) return 200f; if(mileageWeight.containsKey(mileage)) return 10000+mileageWeight.get(mileage); if(goodYear.contains(year)) return (float)Math.exp(2d);   if(year==thisYear) return 87f   ; return  _INNER_SCORE;"
                },
                
                "values":{
                     "goodYear":[1996,1997],
                     "thisYear":2001,
                     "mileageWeight":{"key":[11400,11000],"value":[777.9, 10.2]},
                    "yearcolor":{"key":[1998],"value":["red"]},
                    "colorweight":{"key":["red"],"value":[335.5]},
                    "categorycolor":{"key":["compact"],"value":["red"]},
                    "coolTag":"cool"
                }
            }
        }
    },
    "from": 0,
    "size": 6,
    "explain": false,
    "fetchStored": false,
    "sort":["_score"]
}



// Advanded usage of weighted multi-facet relevance:
 {
    "query": {
        "query_string": {
            "query": "java",
            "relevance":{
                
                    "model":{
                        "variables":{
                             "string":["skill"]
                            },
                        "facets":{
                             "wmstring":["user_skills"] 
                            },
                        "function_params":["_INNER_SCORE",  "user_skills", "skill"],  
                        "function":" int weight = 0; if(user_skills.hasWeight(skill)) weight = user_skills.getWeight(); return  _INNER_SCORE + weight;"
                    },
                    
                    "values":{
                         "skill":"java"
                    }
                }
        }
    },
    "selections": [
    {
        "terms": {
            "country_code": {
                "values": ["us"],
                "excludes": [],
                "operator": "or"
            }
        }
    }],
    "from": 0,
    "size": 10,
    "explain": false,
    "fetchStored": false
}


 
* 
* */
  
  public static CustomMathModel createCustomMathScorer(JSONObject jsonModel, DataTable dataTable) throws JSONException
  {
    CustomMathModel cMathModel = null;
    
    if(jsonModel == null)
      throw new JSONException("No model is specified.");
    
    JSONObject jsonVariables = jsonModel.optJSONObject(JSONConstants.KW_VARIABLES);
    JSONObject jsonFacets = jsonModel.optJSONObject(JSONConstants.KW_FACETS);
    
    //process the function body and parameters firstly;
    
    JSONArray jsonFuncParameter = jsonModel.optJSONArray(JSONConstants.KW_FUNC_PARAMETERS);
    for(int j=0; j<jsonFuncParameter.length(); j++)
    {
      String paramName = jsonFuncParameter.optString(j);
      dataTable.lls_params.add(paramName);
    }
    
    dataTable.funcBody = jsonModel.optString(JSONConstants.KW_FUNCTION);
    
    //process facet variables;
    int[] facetIndice = new int[]{0,0};  // store the facetIndex and facetMultiIndex;
    Iterator<String> it_facet = jsonFacets.keys();
    while(it_facet.hasNext())
    {
      String facetType = it_facet.next();
      JSONArray facetArray = jsonFacets.getJSONArray(facetType);
      handleFacetSymbols(facetType, facetArray, facetIndice, dataTable);
    }
    
    //process other variables;
    Iterator<String> it_var = jsonVariables.keys();
    while(it_var.hasNext())
    {
      String type = it_var.next();
      JSONArray varArray = jsonVariables.getJSONArray(type);
      
      //process set variable;
      if(JSONConstants.KW_TYPE_SET_INT.equals(type) || JSONConstants.KW_TYPE_SET_DOUBLE.equals(type) || JSONConstants.KW_TYPE_SET_LONG.equals(type) || JSONConstants.KW_TYPE_SET_FLOAT.equals(type) || JSONConstants.KW_TYPE_SET_STRING.equals(type))
      {
        JSONArray sets = jsonVariables.getJSONArray(type);
        for(int i=0; i<sets.length(); i++)
        {
          String symbol = sets.getString(i);
          
          if(symbol.equals(JSONConstants.KW_INNER_SCORE) || symbol.equals(JSONConstants.KW_NOW))
            throw new JSONException("variable name can not be reserved keyword.");
          
          
          if(JSONConstants.KW_TYPE_SET_INT.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_SET_INT);
          }
          else if (JSONConstants.KW_TYPE_SET_DOUBLE.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_SET_DOUBLE);
          }
          else if (JSONConstants.KW_TYPE_SET_FLOAT.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_SET_FLOAT);
          }
          else if (JSONConstants.KW_TYPE_SET_LONG.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_SET_LONG);
          }
          else if (JSONConstants.KW_TYPE_SET_STRING.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_SET_STRING);
          }
          
        }
      } // end of set variable;
      
      
      //process map variable;
      else if(type.startsWith(JSONConstants.KW_TYPE_MAP_HEAD))
      {
        JSONArray sets = jsonVariables.getJSONArray(type);
        for(int i=0; i<sets.length(); i++)
        {
          String symbol = sets.getString(i);
          
          if(symbol.equals(JSONConstants.KW_INNER_SCORE) || symbol.equals(JSONConstants.KW_NOW))
            throw new JSONException("variable name can not be reserved keyword.");
          
//          "j":{"key":[1,2,3], "value":[2.3, 3.4, 2.9]}      // a user input hashmap;
          
          
          if(JSONConstants.KW_TYPE_MAP_INT_INT.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_MAP_INT_INT);
          }
          else if (JSONConstants.KW_TYPE_MAP_INT_DOUBLE.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_MAP_INT_DOUBLE);
          }
          else if (JSONConstants.KW_TYPE_MAP_INT_FLOAT.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_MAP_INT_FLOAT);
          }
          else if (JSONConstants.KW_TYPE_MAP_INT_LONG.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_MAP_INT_LONG);
          }
          else if (JSONConstants.KW_TYPE_MAP_INT_STRING.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_MAP_INT_STRING);
          }
          
          else if(JSONConstants.KW_TYPE_MAP_STRING_INT.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_MAP_STRING_INT);
          }
          else if (JSONConstants.KW_TYPE_MAP_STRING_DOUBLE.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_MAP_STRING_DOUBLE);
          }
          else if (JSONConstants.KW_TYPE_MAP_STRING_FLOAT.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_MAP_STRING_FLOAT);
          }
          else if (JSONConstants.KW_TYPE_MAP_STRING_LONG.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_MAP_STRING_LONG);
          }
          else if (JSONConstants.KW_TYPE_MAP_STRING_STRING.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_MAP_STRING_STRING);
          }
        }
      } // end of map variable;
      
      // process normal variable;
      // int, string, double, long
      else if(JSONConstants.KW_TYPE_INT.equals(type) || JSONConstants.KW_TYPE_DOUBLE.equals(type) || JSONConstants.KW_TYPE_LONG.equals(type) || JSONConstants.KW_TYPE_FLOAT.equals(type) || JSONConstants.KW_TYPE_STRING.equals(type) || JSONConstants.KW_TYPE_BOOL.equals(type))
      {
        JSONArray sets = jsonVariables.getJSONArray(type);
        
        for(int i=0; i< sets.length(); i++)
        {
        
          String symbol = sets.getString(i);
          
          if(symbol.equals(JSONConstants.KW_INNER_SCORE) || symbol.equals(JSONConstants.KW_NOW))
            throw new JSONException("variable name can not be reserved keyword.");

          if(JSONConstants.KW_TYPE_INT.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_INT);
          }
          else if (JSONConstants.KW_TYPE_DOUBLE.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_DOUBLE);
          }
          else if (JSONConstants.KW_TYPE_FLOAT.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_FLOAT);
          }
          else if (JSONConstants.KW_TYPE_LONG.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_LONG);
          }
          else if (JSONConstants.KW_TYPE_STRING.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_STRING);
          }
          else if(JSONConstants.KW_TYPE_BOOL.equals(type))
          {
            dataTable.hm_type.put(symbol, JSONConstants.TYPE_BOOLEAN);
          }
        }
      }// end of normal variable while;
    }

    // add now variable;
    String symbolNow = JSONConstants.KW_NOW;
    long now = System.currentTimeMillis();
    dataTable.hm_var.put(symbolNow, now);
    dataTable.hm_type.put(symbolNow, JSONConstants.TYPE_LONG);
    
    // add innerscore;
    String symbolInnerScore = JSONConstants.KW_INNER_SCORE; 
    dataTable.hm_var.put(symbolInnerScore, symbolInnerScore);
    dataTable.hm_type.put(symbolInnerScore, JSONConstants.TYPE_INNER_SCORE);
    
    
    
    
    if(dataTable.funcBody == null || dataTable.funcBody.length()==0)
      throw new JSONException("No function body found.");
    
    if(dataTable.funcBody.indexOf("return ")==-1)
      throw new JSONException("No return statement in the function body.");
   

    //check if all the parameters have defined;
    for(int i=0; i< dataTable.lls_params.size(); i++)
    {
      String symbol = dataTable.lls_params.get(i);
      if( !dataTable.hm_type.containsKey(symbol))
        throw new JSONException("function parameter: " + symbol + " was not defined.");
      
      String type = dataTable.hm_type.get(symbol);
      if(type.startsWith(JSONConstants.TYPE_FACET_HEAD))
      {
        if( (!dataTable.hm_symbol_facet.containsKey(symbol)) && (!dataTable.hm_symbol_mfacet.containsKey(symbol)))
          throw new JSONException("function parameter: " + symbol + " was not defined.");
      }
    }

    dataTable.lls_params = filterParameters(dataTable);
    

    
    
    //compile the math model below;
    String paramString = getParamString(dataTable);
    dataTable.classIDString = dataTable.funcBody + paramString;
    String className = "CRel"+ dataTable.classIDString.hashCode();
    logger.info("Custom relevance math class name is:"+ className);
    

    if(hmModels.containsKey(className))
    {
      cMathModel = hmModels.get(className);
      logger.info("get math model from hashmap:"+ className);
      return cMathModel;
    }
    else
    {

      synchronized(RelevanceQuery.class)
      {
        
        if(hmModels.containsKey(className))
        {
          cMathModel = hmModels.get(className);
          logger.info("get math model from hashmap:"+ className);
          return cMathModel;
        }
        
        
        CtClass ch = CompilationHelper.pool.makeClass(className);
        
        CtClass ci;
        try
        {
          ci = CompilationHelper.pool.get("com.senseidb.search.relevance.CustomMathModel");
        }
        catch (NotFoundException e)
        {
          logger.info(e.getMessage());
          throw new JSONException(e);
        }
        
        ch.addInterface(ci);
        String functionString = makeFuncString(dataTable);
        
        addFacilityMethods(ch);
        
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
          h = CompilationHelper.pool.toClass(ch, new CompilationHelper.CustomLoader(RelevanceQuery.class.getClassLoader(), className));
        }
        catch (CannotCompileException e)
        {
          if(hmModels.containsKey(className))
          {
            cMathModel = hmModels.get(className);
            logger.info("get math model from hashmap:"+ className);
            return cMathModel;
          }
          else
          {
            logger.info(e.getMessage());
            throw new JSONException(e);
          }
        }
        
        try
        {
          cMathModel = (CustomMathModel)h.newInstance();
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
        
        if(hmModels.size() > MAX_NUM_MODELS)
          hmModels = new HashMap<String, CustomMathModel>();
        
        hmModels.put(className, cMathModel);
        logger.info("get math model by compilation:"+ className);
        return cMathModel;
      }        
    }
    
  }
  
  public static void initialize(JSONObject jsonValues, DataTable dataTable) throws JSONException
  {
    HashMap<String, String> hm_type = dataTable.hm_type;
    Iterator it = hm_type.keySet().iterator();
    while(it.hasNext()){
      String symbol = (String)it.next();
      String type = dataTable.hm_type.get(symbol);
      
      if(symbol.equals(JSONConstants.KW_INNER_SCORE) || symbol.equals(JSONConstants.KW_NOW))
        continue;
      
      //process set variable;
      if(JSONConstants.TYPE_SET_INT.equals(type) || JSONConstants.TYPE_SET_DOUBLE.equals(type) || JSONConstants.TYPE_SET_LONG.equals(type) || JSONConstants.TYPE_SET_FLOAT.equals(type) || JSONConstants.TYPE_SET_STRING.equals(type))
      {
        Set hs = null;
        JSONArray values = jsonValues.optJSONArray(symbol);
        
        if(values == null)
          throw new JSONException("variable "+ symbol + " does not have value.");
        
        for (int k =0; k < values.length(); k++){
          if(JSONConstants.TYPE_SET_INT.equals(type))
          {
            if(hs == null)
              hs = new IntOpenHashSet();
            hs.add(values.getInt(k));
          }
          else if (JSONConstants.TYPE_SET_DOUBLE.equals(type))
          {
            if(hs == null)
              hs = new DoubleOpenHashSet();
            hs.add(values.getDouble(k));
          }
          else if (JSONConstants.TYPE_SET_FLOAT.equals(type))
          {
            if(hs == null)
              hs = new FloatOpenHashSet();
            hs.add((float)values.getDouble(k));
          }
          else if (JSONConstants.TYPE_SET_LONG.equals(type))
          {
            if(hs == null)
              hs = new LongOpenHashSet();
            hs.add(Long.parseLong(values.getString(k)));
          }
          else if (JSONConstants.TYPE_SET_STRING.equals(type))
          {
            if(hs == null)
              hs = new ObjectOpenHashSet();
            hs.add(values.getString(k));
          }
        }
        
        dataTable.hm_var.put(symbol, hs);
          
      } // end of set variable;
     
      
      else if(type.startsWith(JSONConstants.TYPE_MAP_HEAD))
      {
          
//          "j":{"key":[1,2,3], "value":[2.3, 3.4, 2.9]}      // a user input hashmap;
          
          JSONObject values = jsonValues.optJSONObject(symbol);
          
          if(values == null)
            throw new JSONException("variable "+ symbol + " does not have value.");
          
          JSONArray keysList = values.optJSONArray("key");
          JSONArray valuesList = values.optJSONArray("value");
          
          if(keysList == null)
            throw new JSONException("variable " + symbol + "is a map, but does not have a key list");
          
          if(valuesList == null)
            throw new JSONException("variable " + symbol + "is a map, but does not have a value list");
          
          int keySize = keysList.length();
          int valueSize = valuesList.length();
          if(keySize != valueSize)
            throw new JSONException("variable " + symbol + ": key size is different from value size, can not convert to a map." );
          
          Map hm = null;
          
          if(JSONConstants.TYPE_MAP_INT_INT.equals(type))
          {
            if(hm == null)
              hm = new Int2IntOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Int2IntOpenHashMap)hm).put(keysList.getInt(j), valuesList.getInt(j));
          }
          else if (JSONConstants.TYPE_MAP_INT_DOUBLE.equals(type))
          {
            if(hm == null)
              hm = new Int2DoubleOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Int2DoubleOpenHashMap)hm).put(keysList.getInt(j), valuesList.getDouble(j));
          }
          else if (JSONConstants.TYPE_MAP_INT_FLOAT.equals(type))
          {
            if(hm == null)
              hm = new Int2FloatOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Int2FloatOpenHashMap)hm).put(keysList.getInt(j), (float)(valuesList.getDouble(j)));
          }
          else if (JSONConstants.TYPE_MAP_INT_LONG.equals(type))
          {
            if(hm == null)
              hm = new Int2LongOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Int2LongOpenHashMap)hm).put(keysList.getInt(j), Long.parseLong(valuesList.getString(j)));
          }
          else if (JSONConstants.TYPE_MAP_INT_STRING.equals(type))
          {
            if(hm == null)
              hm = new Int2ObjectOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Int2ObjectOpenHashMap)hm).put(keysList.getInt(j), valuesList.getString(j));
          }
          
          else if(JSONConstants.TYPE_MAP_STRING_INT.equals(type))
          {
            if(hm == null)
              hm = new Object2IntOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Object2IntOpenHashMap)hm).put(keysList.getString(j), valuesList.getInt(j));
          }
          else if (JSONConstants.TYPE_MAP_STRING_DOUBLE.equals(type))
          {
            if(hm == null)
              hm = new Object2DoubleOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Object2DoubleOpenHashMap)hm).put(keysList.getString(j), valuesList.getDouble(j));
          }
          else if (JSONConstants.TYPE_MAP_STRING_FLOAT.equals(type))
          {
            if(hm == null)
              hm = new Object2FloatOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Object2FloatOpenHashMap)hm).put(keysList.getString(j), (float)(valuesList.getDouble(j)));
          }
          else if (JSONConstants.TYPE_MAP_STRING_LONG.equals(type))
          {
            if(hm == null)
              hm = new Object2LongOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Object2LongOpenHashMap)hm).put(keysList.getString(j), Long.parseLong(valuesList.getString(j)));
          }
          else if (JSONConstants.TYPE_MAP_STRING_STRING.equals(type))
          {
            if(hm == null)
              hm = new Object2ObjectOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Object2ObjectOpenHashMap)hm).put(keysList.getString(j), valuesList.getString(j));
          }
        
          dataTable.hm_var.put(symbol, hm);
          
      } // end of map variable;
      
      else if(JSONConstants.TYPE_INT.equals(type) || JSONConstants.TYPE_DOUBLE.equals(type) || JSONConstants.TYPE_LONG.equals(type) || JSONConstants.TYPE_FLOAT.equals(type) || JSONConstants.TYPE_STRING.equals(type) || JSONConstants.TYPE_BOOLEAN.equals(type))
      {
          
          if( ! jsonValues.has(symbol))
            throw new JSONException("Symbol " + symbol + " was not assigned with a value." );
          
          if(JSONConstants.TYPE_INT.equals(type))
          {
            dataTable.hm_var.put(symbol, jsonValues.getInt(symbol));
          }
          else if (JSONConstants.TYPE_DOUBLE.equals(type))
          {
            dataTable.hm_var.put(symbol, jsonValues.getDouble(symbol));
          }
          else if (JSONConstants.TYPE_FLOAT.equals(type))
          {
            dataTable.hm_var.put(symbol, ((float)jsonValues.getDouble(symbol)));
          }
          else if (JSONConstants.TYPE_LONG.equals(type))
          {
            dataTable.hm_var.put(symbol, Long.parseLong(jsonValues.getString(symbol)));
          }
          else if (JSONConstants.TYPE_STRING.equals(type))
          {
            dataTable.hm_var.put(symbol, jsonValues.getString(symbol));
          }
          else if(JSONConstants.TYPE_BOOLEAN.equals(type))
          {
            dataTable.hm_var.put(symbol, jsonValues.getBoolean(symbol));
          }
      }// end of normal variable while;
      
    }
    
    
    //check if all the parameters have initialized;
    for(int i=0; i< dataTable.lls_params.size(); i++)
    {
      String symbol = dataTable.lls_params.get(i);
      String type = dataTable.hm_type.get(symbol);
      if( !type.startsWith(JSONConstants.TYPE_FACET_HEAD))
      {
        if(!dataTable.hm_var.containsKey(symbol))
          throw new JSONException("function parameter: " + symbol + " was not initialized.");
      }
    }
    
    
  }

  private static void addFacilityMethods(CtClass ch) throws JSONException
  {
    String expStrInt = createEXpStringInt();
    String expStrDouble = createEXpStringDouble();
    String expStrFloat = createEXpStringFloat();
    
    addMethod(expStrInt, ch);
    addMethod(expStrDouble, ch);
    addMethod(expStrFloat, ch);
  }


  private static  void addMethod(String expStr, CtClass ch) throws JSONException
  {
    CtMethod m_exp;
    try
    {
      m_exp = CtNewMethod.make(expStr, ch);
    }
    catch (CannotCompileException e)
    {
      logger.info(e.getMessage());
      throw new JSONException(e);
    }
    
    try
    {
      ch.addMethod(m_exp);
    }
    catch (CannotCompileException e)
    {
      logger.info(e.getMessage());
      throw new JSONException(e);
    }    
  }




  private static String createEXpStringInt()
  {
    StringBuffer sb = new StringBuffer();
    sb.append("public double exp(int val) { return Double.longBitsToDouble(((long) (1512775 * val + 1072632447)) << 32); }");
    return sb.toString();
  }

  private static String createEXpStringDouble()
  {
    StringBuffer sb = new StringBuffer();
    sb.append("public double exp(double val) { return Double.longBitsToDouble(((long) (1512775 * val + 1072632447)) << 32); }");
    return sb.toString();
  }

  private static String createEXpStringFloat()
  {
    StringBuffer sb = new StringBuffer();
    sb.append("public double exp(float val) { return Double.longBitsToDouble(((long) (1512775 * val + 1072632447)) << 32); }");
    return sb.toString();
  }

  private static String getParamString(DataTable dataTable)
  {
    StringBuilder sb = new StringBuilder();
    for(String param : dataTable.lls_params)
    {
      sb.append(param);
      sb.append("#");
      sb.append(dataTable.hm_type.get(param));
      sb.append("#");
    }
    return sb.toString();
  }


  private static LinkedList<String> filterParameters(DataTable dataTable)
  {
    LinkedList<String> lls_new = new LinkedList<String>();
    for(String param : dataTable.lls_params)
    {
      if(  !(dataTable.funcBody.indexOf(param) == -1))
        lls_new.add(param);
    }
    return lls_new;
  }

  
  private static void handleFacetSymbols(String facetType, JSONArray facetArray, int[] facetIndice, DataTable dataTable) throws JSONException
  {
    String type = null;
    boolean isMulti = false;
    
    if(JSONConstants.KW_TYPE_FACET_INT.equals(facetType)) 
      type = JSONConstants.TYPE_FACET_INT;
    else if(JSONConstants.KW_TYPE_FACET_SHORT.equals(facetType)) 
      type = JSONConstants.TYPE_FACET_SHORT;
    else if(JSONConstants.KW_TYPE_FACET_DOUBLE.equals(facetType))
      type = JSONConstants.TYPE_FACET_DOUBLE;
    else if(JSONConstants.KW_TYPE_FACET_FLOAT.equals(facetType))
      type = JSONConstants.TYPE_FACET_FLOAT;
    else if(JSONConstants.KW_TYPE_FACET_LONG.equals(facetType))
      type = JSONConstants.TYPE_FACET_LONG;
    else if(JSONConstants.KW_TYPE_FACET_STRING.equals(facetType))
      type = JSONConstants.TYPE_FACET_STRING;
    
    else
    {
      isMulti = true;
     
      // normal multi-facet;
      if(JSONConstants.KW_TYPE_FACET_M_INT.equals(facetType)) 
        type = JSONConstants.TYPE_FACET_M_INT;
      else if(JSONConstants.KW_TYPE_FACET_M_SHORT.equals(facetType)) 
        type = JSONConstants.TYPE_FACET_M_SHORT;
      else if(JSONConstants.KW_TYPE_FACET_M_DOUBLE.equals(facetType))
        type = JSONConstants.TYPE_FACET_M_DOUBLE;
      else if(JSONConstants.KW_TYPE_FACET_M_FLOAT.equals(facetType))
        type = JSONConstants.TYPE_FACET_M_FLOAT;
      else if(JSONConstants.KW_TYPE_FACET_M_LONG.equals(facetType))
        type = JSONConstants.TYPE_FACET_M_LONG;
      else if(JSONConstants.KW_TYPE_FACET_M_STRING.equals(facetType))
        type = JSONConstants.TYPE_FACET_M_STRING;
      
      // weighted multi-facet;
      else if(JSONConstants.KW_TYPE_FACET_WM_INT.equals(facetType)) 
        type = JSONConstants.TYPE_FACET_WM_INT;
      else if(JSONConstants.KW_TYPE_FACET_WM_SHORT.equals(facetType)) 
        type = JSONConstants.TYPE_FACET_WM_SHORT;
      else if(JSONConstants.KW_TYPE_FACET_WM_DOUBLE.equals(facetType))
        type = JSONConstants.TYPE_FACET_WM_DOUBLE;
      else if(JSONConstants.KW_TYPE_FACET_WM_FLOAT.equals(facetType))
        type = JSONConstants.TYPE_FACET_WM_FLOAT;
      else if(JSONConstants.KW_TYPE_FACET_WM_LONG.equals(facetType))
        type = JSONConstants.TYPE_FACET_WM_LONG;
      else if(JSONConstants.KW_TYPE_FACET_WM_STRING.equals(facetType))
        type = JSONConstants.TYPE_FACET_WM_STRING;
    }

    
    if(type == null)
      throw new JSONException("wrong facet type in facet variable definition json");
    
    for(int i=0; i< facetArray.length(); i++)
    {
      String facetName = facetArray.getString(i);
      String symbol = facetName;

      if(dataTable.hm_symbol_facet.containsKey(symbol) || dataTable.hm_symbol_mfacet.containsKey(symbol))
        throw new JSONException("facet Symbol "+ symbol + " already defined." );

      if(dataTable.hm_facet_index.containsKey(facetName) || dataTable.hm_mfacet_index.containsKey(facetName))
        throw new JSONException("facet name "+ facetName + " already assigned to a symbol." );
      
      if(isMulti == false){
        dataTable.hm_symbol_facet.put(symbol, facetName);
        dataTable.hm_facet_index.put(facetName, facetIndice[0]);
        facetIndice[0] = facetIndice[0]+1;
      }
      else
      {  
        dataTable.hm_symbol_mfacet.put(symbol, facetName);
        dataTable.hm_mfacet_index.put(facetName, facetIndice[1]);
        facetIndice[1] = facetIndice[1]+1;
      }
      
      dataTable.hm_type.put(symbol, type);
    }
  }

  private static String makeFuncString(DataTable dataTable) throws JSONException
  {
    
    StringBuffer sb = new StringBuffer();
    sb.append("public float score(short[] shorts, int[] ints, long[] longs, float[] floats, double[] doubles, boolean[] booleans, String[] strings, Set[] sets, Map[] maps, com.senseidb.search.relevance.MFacetInt[] mFacetInts, com.senseidb.search.relevance.MFacetLong[] mFacetLongs, com.senseidb.search.relevance.MFacetFloat[] mFacetFloats, com.senseidb.search.relevance.MFacetDouble[] mFacetDoubles, com.senseidb.search.relevance.MFacetShort[] mFacetShorts, com.senseidb.search.relevance.MFacetString[] mFacetStrings) {");
    
    int short_index = 0,    m_short_index = 0;
    int int_index = 0,      m_int_index = 0;
    int long_index = 0,     m_long_index = 0;
    int float_index = 0,    m_float_index = 0;
    int double_index = 0,   m_double_index = 0;
    int string_index = 0,   m_string_index = 0;
   
    int boolean_index = 0;
    int set_index = 0;
    int map_index = 0;
    
    for(int i=0; i< dataTable.lls_params.size();i++)
    {
      String paramName = dataTable.lls_params.get(i);
      
      if(!dataTable.hm_type.containsKey(paramName) || (dataTable.hm_type.get(paramName) == null))
        throw new JSONException("function arameter " + paramName + " is not defined.");
      
      String paramType = dataTable.hm_type.get(paramName);
      
      if(paramType.equals(JSONConstants.TYPE_INT) || paramType.equals(JSONConstants.TYPE_FACET_INT))
      {
        sb.append(" int " + paramName + " = ints[" + int_index + "]; ");
        int_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_LONG) || paramType.equals(JSONConstants.TYPE_FACET_LONG))
      {
        sb.append(" long " + paramName + " = longs[" + long_index +"];  ");
        long_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_DOUBLE) || paramType.equals(JSONConstants.TYPE_FACET_DOUBLE))
      {
        sb.append(" double " + paramName + " = doubles["+ double_index +"]; ");
        double_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_FLOAT) || paramType.equals(JSONConstants.TYPE_FACET_FLOAT))
      {
        sb.append(" float " + paramName + " = floats["+ float_index +"]; ");
        float_index++;
      }      
      else if(paramType.equals(JSONConstants.TYPE_STRING) || paramType.equals(JSONConstants.TYPE_FACET_STRING))
      {
        sb.append(" String " + paramName + " = strings["+  string_index +"]; ");
        string_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_BOOLEAN))
      {
        sb.append(" boolean " + paramName + " = booleans["+ boolean_index +"]; ");
        boolean_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_FACET_SHORT))
      {
        sb.append(" short " + paramName + " = shorts["+ short_index +"]; ");
        short_index++;
      }
      
      // set
      else if(paramType.equals(JSONConstants.TYPE_SET_INT))
      {
        sb.append(" it.unimi.dsi.fastutil.ints.IntOpenHashSet " + paramName + " = (it.unimi.dsi.fastutil.ints.IntOpenHashSet) sets["+ set_index +"]; ");
        set_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_SET_LONG))
      {
        sb.append(" it.unimi.dsi.fastutil.longs.LongOpenHashSet " + paramName + " = (it.unimi.dsi.fastutil.longs.LongOpenHashSet) sets["+ set_index +"]; ");
        set_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_SET_DOUBLE))
      {
        sb.append(" it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet " + paramName + " = (it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet) sets["+ set_index +"]; ");
        set_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_SET_FLOAT))
      {
        sb.append(" it.unimi.dsi.fastutil.floats.FloatOpenHashSet " + paramName + " = (it.unimi.dsi.fastutil.floats.FloatOpenHashSet) sets["+ set_index +"]; ");
        set_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_SET_STRING))
      {
        sb.append(" it.unimi.dsi.fastutil.objects.ObjectOpenHashSet " + paramName + " = (it.unimi.dsi.fastutil.objects.ObjectOpenHashSet) sets["+ set_index +"]; ");
        set_index++;
      }
      
      // map;
      else if(paramType.equals(JSONConstants.TYPE_MAP_INT_INT))
      {
        sb.append(" it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_MAP_INT_LONG))
      {
        sb.append(" it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_MAP_INT_DOUBLE))
      {
        sb.append(" it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_MAP_INT_FLOAT))
      {
        sb.append(" it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_MAP_INT_STRING))
      {
        sb.append(" it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      
      else if(paramType.equals(JSONConstants.TYPE_MAP_STRING_INT))
      {
        sb.append(" it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_MAP_STRING_LONG))
      {
        sb.append(" it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_MAP_STRING_DOUBLE))
      {
        sb.append(" it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_MAP_STRING_FLOAT))
      {
        sb.append(" it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_MAP_STRING_STRING))
      {
        sb.append(" it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      
      // innerscore
      else if(paramType.equals(JSONConstants.TYPE_INNER_SCORE))
      {
        sb.append(" float " + paramName + " = floats["+ float_index +"]; ");
        float_index++;
      }
      //multi-facet;
      //com.senseidb.search.relevance.MFacetInt[] mFacetInts, com.senseidb.search.relevance.MFacetLong[] mFacetLongs, com.senseidb.search.relevance.MFacetFloat[] mFacetFloats, , com.senseidb.search.relevance.MFacetShort[] mFacetShorts, com.senseidb.search.relevance.MFacetString[] mFacetStrings
      else if(paramType.equals(JSONConstants.TYPE_FACET_M_DOUBLE))
      {
        sb.append(" com.senseidb.search.relevance.MFacetDouble " + paramName + " = mFacetDoubles["+ m_double_index +"]; ");
        m_double_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_FACET_M_FLOAT))
      {
        sb.append(" com.senseidb.search.relevance.MFacetFloat " + paramName + " = mFacetFloats["+ m_float_index +"]; ");
        m_float_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_FACET_M_INT))
      {
        sb.append(" com.senseidb.search.relevance.MFacetInt " + paramName + " = mFacetInts["+ m_int_index +"]; ");
        m_int_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_FACET_M_LONG))
      {
        sb.append(" com.senseidb.search.relevance.MFacetLong " + paramName + " = mFacetLongs["+ m_long_index +"]; ");
        m_long_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_FACET_M_SHORT))
      {
        sb.append(" com.senseidb.search.relevance.MFacetShort " + paramName + " = mFacetShorts["+ m_short_index +"]; ");
        m_short_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_FACET_M_STRING))
      {
        sb.append(" com.senseidb.search.relevance.MFacetString " + paramName + " = mFacetStrings["+ m_string_index +"]; ");
        m_string_index++;
      }
      
      
      //weighted multi-facet;
      else if(paramType.equals(JSONConstants.TYPE_FACET_WM_DOUBLE))
      {
        sb.append(" com.senseidb.search.relevance.WeightedMFacetDouble " + paramName + " = (com.senseidb.search.relevance.WeightedMFacetDouble) mFacetDoubles["+ m_double_index +"]; ");
        m_double_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_FACET_WM_FLOAT))
      {
        sb.append(" com.senseidb.search.relevance.WeightedMFacetFloat " + paramName + " = (com.senseidb.search.relevance.WeightedMFacetFloat) mFacetFloats["+ m_float_index +"]; ");
        m_float_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_FACET_WM_INT))
      {
        sb.append(" com.senseidb.search.relevance.WeightedMFacetInt " + paramName + " = (com.senseidb.search.relevance.WeightedMFacetInt) mFacetInts["+ m_int_index +"]; ");
        m_int_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_FACET_WM_LONG))
      {
        sb.append(" com.senseidb.search.relevance.WeightedMFacetLong " + paramName + " = (com.senseidb.search.relevance.WeightedMFacetLong) mFacetLongs["+ m_long_index +"]; ");
        m_long_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_FACET_WM_SHORT))
      {
        sb.append(" com.senseidb.search.relevance.WeightedMFacetShort " + paramName + " = (com.senseidb.search.relevance.WeightedMFacetShort) mFacetShorts["+ m_short_index +"]; ");
        m_short_index++;
      }
      else if(paramType.equals(JSONConstants.TYPE_FACET_WM_STRING))
      {
        sb.append(" com.senseidb.search.relevance.WeightedMFacetString " + paramName + " = (com.senseidb.search.relevance.WeightedMFacetString) mFacetStrings["+ m_string_index +"]; ");
        m_string_index++;
      }
    }
    
    sb.append(dataTable.funcBody);
    sb.append("}");
    return sb.toString();
  }


  
  
  
  
  
  public static class CustomLoader extends ClassLoader {

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
  
  public static class DataTable {
    
    //dynamic data;
    public HashMap<String, Object> hm_var;
    
    //static model data;
    public HashMap<String, String> hm_type;
    public HashMap<String, String> hm_symbol_facet;
    public HashMap<String, Integer> hm_facet_index;
    public HashMap<String, String> hm_symbol_mfacet;  //multi-facet
    public HashMap<String, Integer> hm_mfacet_index; //multi-facet 
    
    public LinkedList<String> lls_params;
    public String funcBody = null;
    public String classIDString = null;
    
    public DataTable(){
      hm_var = new HashMap<String, Object>();
      hm_type = new HashMap<String, String>();
      hm_symbol_facet = new HashMap<String, String>();
      hm_facet_index = new HashMap<String, Integer>();
      hm_symbol_mfacet = new HashMap<String, String>();  //multi-facet
      hm_mfacet_index = new HashMap<String, Integer>(); //multi-facet 
      lls_params = new LinkedList<String>();
    }
  }
}
