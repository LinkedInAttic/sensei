package com.senseidb.search.relevance;

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javassist.CannotCompileException;
import javassist.ClassClassPath;
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
import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.data.TermDoubleList;
import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermShortList;
import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.util.BigSegmentedArray;
import com.senseidb.search.query.AbstractScoreAdjuster;

public class RelevanceQuery extends AbstractScoreAdjuster
{
  
  /* JSON keywords*/
  
  // (1) json keys;
  public static final String           KW_MODEL                = "model";
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
  
  // map type: [map_int_int, map_int_float, map_int_double, map_int_long, map_int_string]
  public static final String           KW_TYPE_MAP_HEAD            = "map_"; 
  
  public static final String           KW_TYPE_MAP_INT_INT         = "map_int_int";
  public static final String           KW_TYPE_MAP_INT_FLOAT       = "map_int_float";
  public static final String           KW_TYPE_MAP_INT_STRING      = "map_int_string";
  public static final String           KW_TYPE_MAP_INT_DOUBLE      = "map_int_double";
  public static final String           KW_TYPE_MAP_INT_LONG        = "map_int_long";

  public static final String           KW_TYPE_MAP_STRING_INT         = "map_string_int";
  public static final String           KW_TYPE_MAP_STRING_FLOAT       = "map_string_float";
  public static final String           KW_TYPE_MAP_STRING_STRING      = "map_string_string";
  public static final String           KW_TYPE_MAP_STRING_DOUBLE      = "map_string_double";
  public static final String           KW_TYPE_MAP_STRING_LONG        = "map_string_long";
  
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

  // multi-facet type support: [mdouble, mfloat, mint, mlong, mshort, mstring]
  public static final String           KW_TYPE_FACET_M_INT       = "mint";
  public static final String           KW_TYPE_FACET_M_FLOAT     = "mfloat";
  public static final String           KW_TYPE_FACET_M_STRING    = "mstring";
  public static final String           KW_TYPE_FACET_M_DOUBLE    = "mdouble";
  public static final String           KW_TYPE_FACET_M_LONG      = "mlong";
  public static final String           KW_TYPE_FACET_M_SHORT     = "mshort";
  

  // weighted multi-facet type support: [mdouble, mfloat, mint, mlong, mshort, mstring]
  public static final String           KW_TYPE_FACET_WM_INT       = "wmint";
  public static final String           KW_TYPE_FACET_WM_FLOAT     = "wmfloat";
  public static final String           KW_TYPE_FACET_WM_STRING    = "wmstring";
  public static final String           KW_TYPE_FACET_WM_DOUBLE    = "wmdouble";
  public static final String           KW_TYPE_FACET_WM_LONG      = "wmlong";
  public static final String           KW_TYPE_FACET_WM_SHORT     = "wmshort";

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
  
  // hashmap container types:
  private final String                 TYPE_MAP_INT_INT          = "MAP_INT_INT";
  private final String                 TYPE_MAP_INT_LONG         = "MAP_INT_LONG";
  private final String                 TYPE_MAP_INT_DOUBLE       = "MAP_INT_DOUBLE";
  private final String                 TYPE_MAP_INT_FLOAT        = "MAP_INT_FLOAT";
  private final String                 TYPE_MAP_INT_STRING       = "MAP_INT_STRING";
  private final String                 TYPE_MAP_STRING_INT          = "MAP_STRING_INT";
  private final String                 TYPE_MAP_STRING_LONG         = "MAP_STRING_LONG";
  private final String                 TYPE_MAP_STRING_DOUBLE       = "MAP_STRING_DOUBLE";
  private final String                 TYPE_MAP_STRING_FLOAT        = "MAP_STRING_FLOAT";
  private final String                 TYPE_MAP_STRING_STRING       = "MAP_STRING_STRING";
  
  private final String                 TYPE_MAP_HEAD         = "MAP";

  // (3) facet types:
  private final String                 TYPE_FACET_INT    = "FACET_INT";
  private final String                 TYPE_FACET_LONG   = "FACET_LONG";
  private final String                 TYPE_FACET_DOUBLE = "FACET_DOUBLE";
  private final String                 TYPE_FACET_FLOAT  = "FACET_FLOAT";
  private final String                 TYPE_FACET_SHORT  = "FACET_SHORT";
  private final String                 TYPE_FACET_STRING = "FACET_STRING";
  
  // (4) multi-facet types:
  private final String                 TYPE_FACET_M_INT    = "FACET_M_INT";
  private final String                 TYPE_FACET_M_LONG   = "FACET_M_LONG";
  private final String                 TYPE_FACET_M_DOUBLE = "FACET_M_DOUBLE";
  private final String                 TYPE_FACET_M_FLOAT  = "FACET_M_FLOAT";
  private final String                 TYPE_FACET_M_SHORT  = "FACET_M_SHORT";
  private final String                 TYPE_FACET_M_STRING = "FACET_M_STRING";
  
  // (4) weighted multi-facet types:
  private final String                 TYPE_FACET_WM_INT    = "FACET_WM_INT";
  private final String                 TYPE_FACET_WM_LONG   = "FACET_WM_LONG";
  private final String                 TYPE_FACET_WM_DOUBLE = "FACET_WM_DOUBLE";
  private final String                 TYPE_FACET_WM_FLOAT  = "FACET_WM_FLOAT";
  private final String                 TYPE_FACET_WM_SHORT  = "FACET_WM_SHORT";
  private final String                 TYPE_FACET_WM_STRING = "FACET_WM_STRING";
  
  private final String                 TYPE_FACET_HEAD    = "FACET";
  private final String                 TYPE_M_FACET_HEAD  = "FACET_M";
  private final String                 TYPE_WM_FACET_HEAD = "FACET_WM";
  
  
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
  private final int                 TYPENUMBER_MAP          = 8;
  
  // (3) facet type numbers;
  private final int                 TYPENUMBER_FACET_INT    = 10;
  private final int                 TYPENUMBER_FACET_LONG   = 11;
  private final int                 TYPENUMBER_FACET_DOUBLE = 12;
  private final int                 TYPENUMBER_FACET_FLOAT  = 13;
  private final int                 TYPENUMBER_FACET_SHORT  = 14;
  private final int                 TYPENUMBER_FACET_STRING = 15;
  
  // (4) multi-facet type numbers;
  private final int                 TYPENUMBER_FACET_M_INT    = 20;
  private final int                 TYPENUMBER_FACET_M_LONG   = 21;
  private final int                 TYPENUMBER_FACET_M_DOUBLE = 22;
  private final int                 TYPENUMBER_FACET_M_FLOAT  = 23;
  private final int                 TYPENUMBER_FACET_M_SHORT  = 24;
  private final int                 TYPENUMBER_FACET_M_STRING = 25;
  
  // (5) weighted multi-facet type numbers;
  private final int                 TYPENUMBER_FACET_WM_INT    = 30;
  private final int                 TYPENUMBER_FACET_WM_LONG   = 31;
  private final int                 TYPENUMBER_FACET_WM_DOUBLE = 32;
  private final int                 TYPENUMBER_FACET_WM_FLOAT  = 33;
  private final int                 TYPENUMBER_FACET_WM_SHORT  = 34;
  private final int                 TYPENUMBER_FACET_WM_STRING = 35;
  
  private static final long serialVersionUID = 1L;
  
  public static int MAX_NUM_MODELS  = 10000;
  
  private static Logger logger = Logger.getLogger(RelevanceQuery.class);
  
  protected final Query _query;
  
  private HashMap<String, Object> hm_var = new HashMap<String, Object>();
  private HashMap<String, String> hm_type = new HashMap<String, String>();
  private HashMap<String, String> hm_symbol_facet = new HashMap<String, String>();
  private HashMap<String, Integer> hm_facet_index = new HashMap<String, Integer>();
  private HashMap<String, String> hm_symbol_mfacet = new HashMap<String, String>();  //multi-facet
  private HashMap<String, Integer> hm_mfacet_index = new HashMap<String, Integer>(); //multi-facet 
  
  private LinkedList<String> lls_params = new LinkedList<String>();
  private String funcBody = null;
  private String classIDString = null;
  private CustomScorer cscorer = null;
  private int facetIndex = 0;
  private int facetMultiIndex = 0;
  
  
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
    hs_safe.add("com.senseidb.search.relevance.CustomScorer");
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
  
  
  static HashMap<String, CustomScorer> hmModels = new HashMap<String, CustomScorer>();
  
//  "relevance":{
//    
//                // (1) Model definition part; this json is used to define a model (input variables, columns/facets, and function parameters and body);    
//                "model":{
//               
//                  "variables": {
//                                 "set_int":["c","d"],  // supported hashset types: [set_int, set_float, set_string, set_double, set_long]
//                                 "map_int_float":["j"],  // currently supported hashmap: [map_int_float, map_int_double, map_int_*...] [map_string_int, map_string_float, map_string_*]
//                                 "int":["e","f"],       // supported normal variables: [int, double, float, long, bool, string]
//                                 "long":["g","h"]
//                                },
//                  "facets":{
//                               "int":["year","age"],   // facet type support: [double, float, int, long, short, string];
//                               "long":["time"]         // facet variable has the same name as the facet name, and they are defined inside this json;
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
//                 },
//                 
//                 //(2) Input values for the model above, if the model requires input values;
//                 "values":{
//                   "c":[1996,1997],
//                   "e":0.98,
//                   "j":{"key":[1,2,3], "value":[2.3, 3.4, 2.9]}      // a user input hashmap;
//                 }
//            }
  
  
  
  
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
  
  public RelevanceQuery(Query query, JSONObject relevance) throws JSONException
  {
    super(query);
    _query = query;
    preprocess(relevance);
  }
 



  private void preprocess(JSONObject relevance) throws JSONException
  {
    JSONObject jsonModel = relevance.optJSONObject(KW_MODEL);
    if(jsonModel == null)
      throw new JSONException("No model is specified.");
    
    JSONObject jsonVariables = jsonModel.optJSONObject(KW_VARIABLES);
    JSONObject jsonFacets = jsonModel.optJSONObject(KW_FACETS);
    
    JSONObject jsonValues = relevance.optJSONObject(KW_VALUES);  // the json containing the values, could be null;
    
    
    //process the function body and parameters firstly;
    
    JSONArray jsonFuncParameter = jsonModel.optJSONArray(KW_FUNC_PARAMETERS);
    for(int j=0; j<jsonFuncParameter.length(); j++)
    {
      String paramName = jsonFuncParameter.optString(j);
      lls_params.add(paramName);
    }
    
    funcBody = jsonModel.optString(KW_FUNCTION);
    
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
          
          if(symbol.equals(KW_INNER_SCORE) || symbol.equals(KW_NOW))
            throw new JSONException("variable name can not be reserved keyword.");
          
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
              hs.add(Long.parseLong(values.getString(k)));
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
      
      
      //process map variable;
      else if(type.startsWith(KW_TYPE_MAP_HEAD))
      {
        JSONArray sets = jsonVariables.getJSONArray(type);
        for(int i=0; i<sets.length(); i++)
        {
          String symbol = sets.getString(i);
          
          if(symbol.equals(KW_INNER_SCORE) || symbol.equals(KW_NOW))
            throw new JSONException("variable name can not be reserved keyword.");
          
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
          
          if(KW_TYPE_MAP_INT_INT.equals(type))
          {
            if(hm == null)
              hm = new Int2IntOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Int2IntOpenHashMap)hm).put(keysList.getInt(j), valuesList.getInt(j));
            hm_type.put(symbol, TYPE_MAP_INT_INT);
          }
          else if (KW_TYPE_MAP_INT_DOUBLE.equals(type))
          {
            if(hm == null)
              hm = new Int2DoubleOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Int2DoubleOpenHashMap)hm).put(keysList.getInt(j), valuesList.getDouble(j));
            hm_type.put(symbol, TYPE_MAP_INT_DOUBLE);
          }
          else if (KW_TYPE_MAP_INT_FLOAT.equals(type))
          {
            if(hm == null)
              hm = new Int2FloatOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Int2FloatOpenHashMap)hm).put(keysList.getInt(j), (float)(valuesList.getDouble(j)));
            hm_type.put(symbol, TYPE_MAP_INT_FLOAT);
          }
          else if (KW_TYPE_MAP_INT_LONG.equals(type))
          {
            if(hm == null)
              hm = new Int2LongOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Int2LongOpenHashMap)hm).put(keysList.getInt(j), Long.parseLong(valuesList.getString(j)));
            hm_type.put(symbol, TYPE_MAP_INT_LONG);
          }
          else if (KW_TYPE_MAP_INT_STRING.equals(type))
          {
            if(hm == null)
              hm = new Int2ObjectOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Int2ObjectOpenHashMap)hm).put(keysList.getInt(j), valuesList.getString(j));
            hm_type.put(symbol, TYPE_MAP_INT_STRING);
          }
          
          else if(KW_TYPE_MAP_STRING_INT.equals(type))
          {
            if(hm == null)
              hm = new Object2IntOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Object2IntOpenHashMap)hm).put(keysList.getString(j), valuesList.getInt(j));
            hm_type.put(symbol, TYPE_MAP_STRING_INT);
          }
          else if (KW_TYPE_MAP_STRING_DOUBLE.equals(type))
          {
            if(hm == null)
              hm = new Object2DoubleOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Object2DoubleOpenHashMap)hm).put(keysList.getString(j), valuesList.getDouble(j));
            hm_type.put(symbol, TYPE_MAP_STRING_DOUBLE);
          }
          else if (KW_TYPE_MAP_STRING_FLOAT.equals(type))
          {
            if(hm == null)
              hm = new Object2FloatOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Object2FloatOpenHashMap)hm).put(keysList.getString(j), (float)(valuesList.getDouble(j)));
            hm_type.put(symbol, TYPE_MAP_STRING_FLOAT);
          }
          else if (KW_TYPE_MAP_STRING_LONG.equals(type))
          {
            if(hm == null)
              hm = new Object2LongOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Object2LongOpenHashMap)hm).put(keysList.getString(j), Long.parseLong(valuesList.getString(j)));
            hm_type.put(symbol, TYPE_MAP_STRING_LONG);
          }
          else if (KW_TYPE_MAP_STRING_STRING.equals(type))
          {
            if(hm == null)
              hm = new Object2ObjectOpenHashMap();
            for(int j=0; j<keySize; j++)
              ((Object2ObjectOpenHashMap)hm).put(keysList.getString(j), valuesList.getString(j));
            hm_type.put(symbol, TYPE_MAP_STRING_STRING);
          }
        
          
          if(hm_var.containsKey(symbol))
            throw new JSONException("Symbol "+ symbol + " already defined." );
          
          hm_var.put(symbol, hm);
          
        }
      } // end of map variable;
      
      // process normal variable;
      // int, string, double, long
      else if(KW_TYPE_INT.equals(type) || KW_TYPE_DOUBLE.equals(type) || KW_TYPE_LONG.equals(type) || KW_TYPE_FLOAT.equals(type) || KW_TYPE_STRING.equals(type) || KW_TYPE_BOOL.equals(type))
      {
        JSONArray sets = jsonVariables.getJSONArray(type);
        
        for(int i=0; i< sets.length(); i++)
        {
        
          String symbol = sets.getString(i);
          
          if(symbol.equals(KW_INNER_SCORE) || symbol.equals(KW_NOW))
            throw new JSONException("variable name can not be reserved keyword.");

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
            hm_var.put(symbol, Long.parseLong(jsonValues.getString(symbol)));
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
        if( (!hm_symbol_facet.containsKey(symbol)) && (!hm_symbol_mfacet.containsKey(symbol)))
          throw new JSONException("function parameter: " + symbol + " was not defined.");
      }
      else
      {
        if(!hm_var.containsKey(symbol))
          throw new JSONException("function parameter: " + symbol + " was not defined.");
      }
    }

    lls_params = filterParameters(lls_params, funcBody);
    
    String paramString = getParamString(lls_params, hm_type);
    
    classIDString = funcBody + paramString;
    String className = "CRel"+ classIDString.hashCode();
    logger.info("Custom relevance class name is:"+ className);
    

    if(hmModels.containsKey(className))
      cscorer = hmModels.get(className);
    else
    {

      synchronized(RelevanceQuery.class)
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
          ci = pool.get("com.senseidb.search.relevance.CustomScorer");
        }
        catch (NotFoundException e)
        {
          logger.info(e.getMessage());
          throw new JSONException(e);
        }
        
        ch.addInterface(ci);
        String functionString = makeFuncString(funcBody, hm_type, lls_params);
        
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
        
        if(hmModels.size() > MAX_NUM_MODELS)
          hmModels = new HashMap<String, CustomScorer>();
        
        hmModels.put(className, cscorer);
      }        
    }
    
  }

  private void addFacilityMethods(CtClass ch) throws JSONException
  {
    String expStrInt = createEXpStringInt();
    String expStrDouble = createEXpStringDouble();
    String expStrFloat = createEXpStringFloat();
    
    addMethod(expStrInt, ch);
    addMethod(expStrDouble, ch);
    addMethod(expStrFloat, ch);
  }


  private void addMethod(String expStr, CtClass ch) throws JSONException
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




  private String createEXpStringInt()
  {
    StringBuffer sb = new StringBuffer();
    sb.append("public double exp(int val) { return Double.longBitsToDouble(((long) (1512775 * val + 1072632447)) << 32); }");
    return sb.toString();
  }

  private String createEXpStringDouble()
  {
    StringBuffer sb = new StringBuffer();
    sb.append("public double exp(double val) { return Double.longBitsToDouble(((long) (1512775 * val + 1072632447)) << 32); }");
    return sb.toString();
  }

  private String createEXpStringFloat()
  {
    StringBuffer sb = new StringBuffer();
    sb.append("public double exp(float val) { return Double.longBitsToDouble(((long) (1512775 * val + 1072632447)) << 32); }");
    return sb.toString();
  }

  private String getParamString(LinkedList<String> lls_params, HashMap<String, String> hm_type)
  {
    StringBuilder sb = new StringBuilder();
    for(String param : lls_params)
    {
      sb.append(param);
      sb.append("#");
      sb.append(hm_type.get(param));
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
    boolean isMulti = false;
    
    if(KW_TYPE_FACET_INT.equals(facetType)) 
      type = TYPE_FACET_INT;
    else if(KW_TYPE_FACET_SHORT.equals(facetType)) 
      type = TYPE_FACET_SHORT;
    else if(KW_TYPE_FACET_DOUBLE.equals(facetType))
      type = TYPE_FACET_DOUBLE;
    else if(KW_TYPE_FACET_FLOAT.equals(facetType))
      type = TYPE_FACET_FLOAT;
    else if(KW_TYPE_FACET_LONG.equals(facetType))
      type = TYPE_FACET_LONG;
    else if(KW_TYPE_FACET_STRING.equals(facetType))
      type = TYPE_FACET_STRING;
    
    else
    {
      isMulti = true;
     
      // normal multi-facet;
      if(KW_TYPE_FACET_M_INT.equals(facetType)) 
        type = TYPE_FACET_M_INT;
      else if(KW_TYPE_FACET_M_SHORT.equals(facetType)) 
        type = TYPE_FACET_M_SHORT;
      else if(KW_TYPE_FACET_M_DOUBLE.equals(facetType))
        type = TYPE_FACET_M_DOUBLE;
      else if(KW_TYPE_FACET_M_FLOAT.equals(facetType))
        type = TYPE_FACET_M_FLOAT;
      else if(KW_TYPE_FACET_M_LONG.equals(facetType))
        type = TYPE_FACET_M_LONG;
      else if(KW_TYPE_FACET_M_STRING.equals(facetType))
        type = TYPE_FACET_M_STRING;
      
      // weighted multi-facet;
      else if(KW_TYPE_FACET_WM_INT.equals(facetType)) 
        type = TYPE_FACET_WM_INT;
      else if(KW_TYPE_FACET_WM_SHORT.equals(facetType)) 
        type = TYPE_FACET_WM_SHORT;
      else if(KW_TYPE_FACET_WM_DOUBLE.equals(facetType))
        type = TYPE_FACET_WM_DOUBLE;
      else if(KW_TYPE_FACET_WM_FLOAT.equals(facetType))
        type = TYPE_FACET_WM_FLOAT;
      else if(KW_TYPE_FACET_WM_LONG.equals(facetType))
        type = TYPE_FACET_WM_LONG;
      else if(KW_TYPE_FACET_WM_STRING.equals(facetType))
        type = TYPE_FACET_WM_STRING;
    }

    
    if(type == null)
      throw new JSONException("wrong facet type in facet variable definition json");
    
    for(int i=0; i< facetArray.length(); i++)
    {
      String facetName = facetArray.getString(i);
      String symbol = facetName;

      if(hm_symbol_facet.containsKey(symbol) || hm_symbol_mfacet.containsKey(symbol))
        throw new JSONException("facet Symbol "+ symbol + " already defined." );

      if(hm_facet_index.containsKey(facetName) || hm_mfacet_index.containsKey(facetName))
        throw new JSONException("facet name "+ facetName + " already assigned to a symbol." );
      
      if(isMulti == false){
        hm_symbol_facet.put(symbol, facetName);
        hm_facet_index.put(facetName, facetIndex);
        facetIndex++;
      }
      else
      {  
        hm_symbol_mfacet.put(symbol, facetName);
        hm_mfacet_index.put(facetName, facetMultiIndex);
        facetMultiIndex++;
      }
      
      hm_type.put(symbol, type);
    }
  }

  private String makeFuncString(String funcBody, 
                                HashMap<String, String> hm_type,
                                LinkedList<String> lls_params) throws JSONException
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
    
    for(int i=0; i< lls_params.size();i++)
    {
      String paramName = lls_params.get(i);
      
      if(!hm_type.containsKey(paramName) || (hm_type.get(paramName) == null))
        throw new JSONException("function arameter " + paramName + " is not defined.");
      
      String paramType = hm_type.get(paramName);
      
      if(paramType.equals(TYPE_INT) || paramType.equals(TYPE_FACET_INT))
      {
        sb.append(" int " + paramName + " = ints[" + int_index + "]; ");
        int_index++;
      }
      else if(paramType.equals(TYPE_LONG) || paramType.equals(TYPE_FACET_LONG))
      {
        sb.append(" long " + paramName + " = longs[" + long_index +"];  ");
        long_index++;
      }
      else if(paramType.equals(TYPE_DOUBLE) || paramType.equals(TYPE_FACET_DOUBLE))
      {
        sb.append(" double " + paramName + " = doubles["+ double_index +"]; ");
        double_index++;
      }
      else if(paramType.equals(TYPE_FLOAT) || paramType.equals(TYPE_FACET_FLOAT))
      {
        sb.append(" float " + paramName + " = floats["+ float_index +"]; ");
        float_index++;
      }      
      else if(paramType.equals(TYPE_STRING) || paramType.equals(TYPE_FACET_STRING))
      {
        sb.append(" String " + paramName + " = strings["+  string_index +"]; ");
        string_index++;
      }
      else if(paramType.equals(TYPE_BOOLEAN))
      {
        sb.append(" boolean " + paramName + " = booleans["+ boolean_index +"]; ");
        boolean_index++;
      }
      else if(paramType.equals(TYPE_FACET_SHORT))
      {
        sb.append(" short " + paramName + " = shorts["+ short_index +"]; ");
        short_index++;
      }
      
      // set
      else if(paramType.equals(TYPE_SET_INT))
      {
        sb.append(" it.unimi.dsi.fastutil.ints.IntOpenHashSet " + paramName + " = (it.unimi.dsi.fastutil.ints.IntOpenHashSet) sets["+ set_index +"]; ");
        set_index++;
      }
      else if(paramType.equals(TYPE_SET_LONG))
      {
        sb.append(" it.unimi.dsi.fastutil.longs.LongOpenHashSet " + paramName + " = (it.unimi.dsi.fastutil.longs.LongOpenHashSet) sets["+ set_index +"]; ");
        set_index++;
      }
      else if(paramType.equals(TYPE_SET_DOUBLE))
      {
        sb.append(" it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet " + paramName + " = (it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet) sets["+ set_index +"]; ");
        set_index++;
      }
      else if(paramType.equals(TYPE_SET_FLOAT))
      {
        sb.append(" it.unimi.dsi.fastutil.floats.FloatOpenHashSet " + paramName + " = (it.unimi.dsi.fastutil.floats.FloatOpenHashSet) sets["+ set_index +"]; ");
        set_index++;
      }
      else if(paramType.equals(TYPE_SET_STRING))
      {
        sb.append(" it.unimi.dsi.fastutil.objects.ObjectOpenHashSet " + paramName + " = (it.unimi.dsi.fastutil.objects.ObjectOpenHashSet) sets["+ set_index +"]; ");
        set_index++;
      }
      
      // map;
      else if(paramType.equals(TYPE_MAP_INT_INT))
      {
        sb.append(" it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      else if(paramType.equals(TYPE_MAP_INT_LONG))
      {
        sb.append(" it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      else if(paramType.equals(TYPE_MAP_INT_DOUBLE))
      {
        sb.append(" it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      else if(paramType.equals(TYPE_MAP_INT_FLOAT))
      {
        sb.append(" it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      else if(paramType.equals(TYPE_MAP_INT_STRING))
      {
        sb.append(" it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      
      else if(paramType.equals(TYPE_MAP_STRING_INT))
      {
        sb.append(" it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      else if(paramType.equals(TYPE_MAP_STRING_LONG))
      {
        sb.append(" it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      else if(paramType.equals(TYPE_MAP_STRING_DOUBLE))
      {
        sb.append(" it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      else if(paramType.equals(TYPE_MAP_STRING_FLOAT))
      {
        sb.append(" it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      else if(paramType.equals(TYPE_MAP_STRING_STRING))
      {
        sb.append(" it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap " + paramName + " = (it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap) maps["+ map_index +"]; ");
        map_index++;
      }
      
      // innerscore
      else if(paramType.equals(TYPE_INNER_SCORE))
      {
        sb.append(" float " + paramName + " = floats["+ float_index +"]; ");
        float_index++;
      }
      //multi-facet;
      //com.senseidb.search.relevance.MFacetInt[] mFacetInts, com.senseidb.search.relevance.MFacetLong[] mFacetLongs, com.senseidb.search.relevance.MFacetFloat[] mFacetFloats, , com.senseidb.search.relevance.MFacetShort[] mFacetShorts, com.senseidb.search.relevance.MFacetString[] mFacetStrings
      else if(paramType.equals(TYPE_FACET_M_DOUBLE))
      {
        sb.append(" com.senseidb.search.relevance.MFacetDouble " + paramName + " = mFacetDoubles["+ m_double_index +"]; ");
        m_double_index++;
      }
      else if(paramType.equals(TYPE_FACET_M_FLOAT))
      {
        sb.append(" com.senseidb.search.relevance.MFacetFloat " + paramName + " = mFacetFloats["+ m_float_index +"]; ");
        m_float_index++;
      }
      else if(paramType.equals(TYPE_FACET_M_INT))
      {
        sb.append(" com.senseidb.search.relevance.MFacetInt " + paramName + " = mFacetInts["+ m_int_index +"]; ");
        m_int_index++;
      }
      else if(paramType.equals(TYPE_FACET_M_LONG))
      {
        sb.append(" com.senseidb.search.relevance.MFacetLong " + paramName + " = mFacetLongs["+ m_long_index +"]; ");
        m_long_index++;
      }
      else if(paramType.equals(TYPE_FACET_M_SHORT))
      {
        sb.append(" com.senseidb.search.relevance.MFacetShort " + paramName + " = mFacetShorts["+ m_short_index +"]; ");
        m_short_index++;
      }
      else if(paramType.equals(TYPE_FACET_M_STRING))
      {
        sb.append(" com.senseidb.search.relevance.MFacetString " + paramName + " = mFacetStrings["+ m_string_index +"]; ");
        m_string_index++;
      }
      
      
      //weighted multi-facet;
      else if(paramType.equals(TYPE_FACET_WM_DOUBLE))
      {
        sb.append(" com.senseidb.search.relevance.WeightedMFacetDouble " + paramName + " = (com.senseidb.search.relevance.WeightedMFacetDouble) mFacetDoubles["+ m_double_index +"]; ");
        m_double_index++;
      }
      else if(paramType.equals(TYPE_FACET_WM_FLOAT))
      {
        sb.append(" com.senseidb.search.relevance.WeightedMFacetFloat " + paramName + " = (com.senseidb.search.relevance.WeightedMFacetFloat) mFacetFloats["+ m_float_index +"]; ");
        m_float_index++;
      }
      else if(paramType.equals(TYPE_FACET_WM_INT))
      {
        sb.append(" com.senseidb.search.relevance.WeightedMFacetInt " + paramName + " = (com.senseidb.search.relevance.WeightedMFacetInt) mFacetInts["+ m_int_index +"]; ");
        m_int_index++;
      }
      else if(paramType.equals(TYPE_FACET_WM_LONG))
      {
        sb.append(" com.senseidb.search.relevance.WeightedMFacetLong " + paramName + " = (com.senseidb.search.relevance.WeightedMFacetLong) mFacetLongs["+ m_long_index +"]; ");
        m_long_index++;
      }
      else if(paramType.equals(TYPE_FACET_WM_SHORT))
      {
        sb.append(" com.senseidb.search.relevance.WeightedMFacetShort " + paramName + " = (com.senseidb.search.relevance.WeightedMFacetShort) mFacetShorts["+ m_short_index +"]; ");
        m_short_index++;
      }
      else if(paramType.equals(TYPE_FACET_WM_STRING))
      {
        sb.append(" com.senseidb.search.relevance.WeightedMFacetString " + paramName + " = (com.senseidb.search.relevance.WeightedMFacetString) mFacetStrings["+ m_string_index +"]; ");
        m_string_index++;
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
      
      //simple facet;
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

      //multi-facet;
      int numMultiFacet = hm_symbol_mfacet.keySet().size();
      final MultiValueFacetDataCache[] mDataCaches = new MultiValueFacetDataCache[numMultiFacet];
      final TermValueList[] mTermLists = new TermValueList[numMultiFacet];
      
      Iterator<String> iter_mfacet = hm_mfacet_index.keySet().iterator();
      while(iter_mfacet.hasNext()){
        String mFacetName = iter_mfacet.next();
        
        // validation;
        Object dataObj = boboReader.getFacetData(mFacetName);
        if ( ! (dataObj instanceof FacetDataCache<?>))
          return innerScorer;
        
        int index = hm_mfacet_index.get(mFacetName);
        mDataCaches[index] = (MultiValueFacetDataCache)(boboReader.getFacetData(mFacetName));
        mTermLists[index] = ((MultiValueFacetDataCache)(boboReader.getFacetData(mFacetName))).valArray;
      }
      
      
      final int paramSize = lls_params.size();
      
      final int[] types = new int[paramSize];  //store each parameter's type;
      final int[] facetIndex = new int[paramSize];  // if this parameter is a facet, what is its index number in the facet data array;
      final int[] arrayIndex = new int[paramSize];  // for each paramter, what is its index number in its own parameter array when passing into the function;
      final int[] mFacetIndex = new int[paramSize];  // if this parameter is a multi-facet, we need to know its index. Since we only use one array to store multi-facet, we do not need array index like the one for the simple facet;
      final int[] mArrayIndex = new int[paramSize];  // for each multi-facet, what is its index number in its own parameter array when passing into the function;
      
      updateArrayIndex(paramSize, types, facetIndex, arrayIndex, mFacetIndex, mArrayIndex);

      return new CodeGenScorer(innerScorer, cscorer, orderArrays, termLists, types, facetIndex, arrayIndex, mDataCaches, mTermLists, mFacetIndex, mArrayIndex, paramSize);
    }
    else{
      return innerScorer;
    }
  }

  
  private void updateArrayIndex(int paramSize, int[] types, int[] facetIndex, int[] arrayIndex, int[] mFacetIndex, int[] mArrayIndex)
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
      
      if(hm_type.get(lls_params.get(i)).equals(TYPE_INNER_SCORE)){
        types[i] = TYPENUMBER_INNER_SCORE;  //inner_score type parameter;
        facetIndex[i] = -1;  //should not be used;
        mFacetIndex[i] = -1;
        arrayIndex[i] = float_index;
        float_index++;
        mArrayIndex[i] = -1;
      }
      else if (hm_type.get(lls_params.get(i)).startsWith(TYPE_FACET_HEAD))
      {
        String type = hm_type.get(lls_params.get(i));
        
        if( (!type.startsWith(TYPE_M_FACET_HEAD)) && (!type.startsWith(TYPE_WM_FACET_HEAD)))
        {
          // non-multi-facet
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
          
          mArrayIndex[i] = -1;
        }
        else 
        {
          // multi-facet or weighted multi-facet;
          isMultiFacet = true;
          
          //normal multi-facet
          if(type.equals(TYPE_FACET_M_INT))
          {
            types[i] = TYPENUMBER_FACET_M_INT;
            mArrayIndex[i] = m_int_index;
            m_int_index++;
          }
          else if (type.equals(TYPE_FACET_M_LONG))
          {
            types[i] = TYPENUMBER_FACET_M_LONG;
            mArrayIndex[i] = m_long_index;
            m_long_index++;
          }
          else if (type.equals(TYPE_FACET_M_DOUBLE))
          {
            types[i] = TYPENUMBER_FACET_M_DOUBLE;
            mArrayIndex[i] = m_double_index;
            m_double_index++;
          }
          else if (type.equals(TYPE_FACET_M_FLOAT))
          {
            types[i] = TYPENUMBER_FACET_M_FLOAT;
            mArrayIndex[i] = m_float_index;
            m_float_index++;
          }
          else if (type.equals(TYPE_FACET_M_SHORT))
          {
            types[i] = TYPENUMBER_FACET_M_SHORT;
            mArrayIndex[i] = m_short_index;
            m_short_index++;
          }
          else if (type.equals(TYPE_FACET_M_STRING))
          {
            types[i] = TYPENUMBER_FACET_M_STRING;
            mArrayIndex[i] = m_string_index;
            m_string_index++;
          }
          
          //weighted multi-facet
          else if(type.equals(TYPE_FACET_WM_INT))
          {
            types[i] = TYPENUMBER_FACET_WM_INT;
            mArrayIndex[i] = m_int_index;
            m_int_index++;
          }
          else if (type.equals(TYPE_FACET_WM_LONG))
          {
            types[i] = TYPENUMBER_FACET_WM_LONG;
            mArrayIndex[i] = m_long_index;
            m_long_index++;
          }
          else if (type.equals(TYPE_FACET_WM_DOUBLE))
          {
            types[i] = TYPENUMBER_FACET_WM_DOUBLE;
            mArrayIndex[i] = m_double_index;
            m_double_index++;
          }
          else if (type.equals(TYPE_FACET_WM_FLOAT))
          {
            types[i] = TYPENUMBER_FACET_WM_FLOAT;
            mArrayIndex[i] = m_float_index;
            m_float_index++;
          }
          else if (type.equals(TYPE_FACET_WM_SHORT))
          {
            types[i] = TYPENUMBER_FACET_WM_SHORT;
            mArrayIndex[i] = m_short_index;
            m_short_index++;
          }
          else if (type.equals(TYPE_FACET_WM_STRING))
          {
            types[i] = TYPENUMBER_FACET_WM_STRING;
            mArrayIndex[i] = m_string_index;
            m_string_index++;
          }
          
          arrayIndex[i] = -1;
        }
        
        if(isMultiFacet == false)
        {
          String facetName = hm_symbol_facet.get(lls_params.get(i));
          int index = hm_facet_index.get(facetName);
          facetIndex[i] = index;  // record the facet index;
          mFacetIndex[i] = -1;
        }
        else
        {
          String mfacetName = hm_symbol_mfacet.get(lls_params.get(i));
          int mIndex = hm_mfacet_index.get(mfacetName);
          facetIndex[i] = -1;
          mFacetIndex[i] = mIndex;  // record the multi-facet index;
        }
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
        else if (type.startsWith(TYPE_MAP_HEAD))
        {
          types[i] = TYPENUMBER_MAP;
          arrayIndex[i] = map_index;
          map_index++;
        }
        
        facetIndex[i] = -1;  // should not be used;
        mFacetIndex[i] = -1;
        mArrayIndex[i] = -1;
      }
    }    
  }


  public class  CodeGenScorer extends Scorer{

    final Scorer _innerScorer;
    final CustomScorer _cscorer;
    
    final BigSegmentedArray[] _orderArrays;
    final TermValueList[] _termLists;
    
    final MultiValueFacetDataCache[] _mDataCaches;
    final TermValueList[] _mTermLists;
    
    final int[] _types;
    final int[] _facetIndex;
    final int[] _arrayIndex;
    
    final int[] _mFacetIndex;
    final int[] _mArrayIndex;
    
    final int _paramSize;
    
    
    final short[] shorts;
    final int[] ints;
    final long[] longs;
    final float[] floats;
    final double[] doubles;
    final boolean[] booleans;
    final String[] strings;
    final Set[] sets;
    final Map[] maps;
    
    final MFacetInt[] mFacetInts;
    final MFacetLong[] mFacetLongs;
    final MFacetShort[] mFacetShorts;
    final MFacetFloat[] mFacetFloats;
    final MFacetDouble[] mFacetDoubles;
    final MFacetString[] mFacetStrings;
    
    final int[] dynamicAR;
    
    public CodeGenScorer(Scorer innerScorer, 
                         CustomScorer cscorer, 
                         BigSegmentedArray[] orderArrays,
                         TermValueList[] termLists,
                         int[] types,
                         
                         int[] facetIndex,
                         int[] arrayIndex,
                         
                         MultiValueFacetDataCache[] mDataCaches, 
                         TermValueList[] mTermLists, 
                         int[] mFacetIndex, 
                         int[] mArrayIndex, 
                         
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
          case TYPENUMBER_MAP:
                    maps[_arrayIndex[i]] = (Map)hm_var.get(lls_params.get(i));
                    break;                    
                    
          
          //multi-facet container initialization; 
          case TYPENUMBER_FACET_M_INT:
                    mFacetInts[_mArrayIndex[i]] =  new MFacetInt(_mDataCaches[_mFacetIndex[i]]);
                    arDynamic.add(i);
                    break;
          case TYPENUMBER_FACET_M_LONG:
                    mFacetLongs[_mArrayIndex[i]] =  new MFacetLong(_mDataCaches[_mFacetIndex[i]]);
                    arDynamic.add(i);
                    break;
          case  TYPENUMBER_FACET_M_DOUBLE:
                    mFacetDoubles[_mArrayIndex[i]] =  new MFacetDouble(_mDataCaches[_mFacetIndex[i]]);
                    arDynamic.add(i);
                    break;
          case TYPENUMBER_FACET_M_FLOAT:
                    mFacetFloats[_mArrayIndex[i]] =  new MFacetFloat(_mDataCaches[_mFacetIndex[i]]);
                    arDynamic.add(i);
                    break;
          case TYPENUMBER_FACET_M_SHORT:
                    mFacetShorts[_mArrayIndex[i]] =  new MFacetShort(_mDataCaches[_mFacetIndex[i]]);
                    arDynamic.add(i);
                    break;
          case TYPENUMBER_FACET_M_STRING:                    
                    mFacetStrings[_mArrayIndex[i]] =  new MFacetString(_mDataCaches[_mFacetIndex[i]]);
                    arDynamic.add(i);
                    break;    
                    
          
          //weighted multi-facet container initialization; 
          case TYPENUMBER_FACET_WM_INT:
                    mFacetInts[_mArrayIndex[i]] =  new WeightedMFacetInt(_mDataCaches[_mFacetIndex[i]]);
                    arDynamic.add(i);
                    break;
          case TYPENUMBER_FACET_WM_LONG:
                    mFacetLongs[_mArrayIndex[i]] =  new WeightedMFacetLong(_mDataCaches[_mFacetIndex[i]]);
                    arDynamic.add(i);
                    break;
          case  TYPENUMBER_FACET_WM_DOUBLE:
                    mFacetDoubles[_mArrayIndex[i]] =  new WeightedMFacetDouble(_mDataCaches[_mFacetIndex[i]]);
                    arDynamic.add(i);
                    break;
          case TYPENUMBER_FACET_WM_FLOAT:
                    mFacetFloats[_mArrayIndex[i]] =  new WeightedMFacetFloat(_mDataCaches[_mFacetIndex[i]]);
                    arDynamic.add(i);
                    break;
          case TYPENUMBER_FACET_WM_SHORT:
                    mFacetShorts[_mArrayIndex[i]] =  new WeightedMFacetShort(_mDataCaches[_mFacetIndex[i]]);
                    arDynamic.add(i);
                    break;
          case TYPENUMBER_FACET_WM_STRING:                    
                    mFacetStrings[_mArrayIndex[i]] =  new WeightedMFacetString(_mDataCaches[_mFacetIndex[i]]);
                    arDynamic.add(i);
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
      
      int docID = docID();
      //update the dynamic parameters only when we have to.
      for(int j=0; j < dynamicAR.length; j++)
      {
        
        // only when the parameter is inner score variable or facet variable, we need to update the score function input parameter arrays; 
        switch (_types[dynamicAR[j]]) {
          case TYPENUMBER_INNER_SCORE:  
                    floats[_arrayIndex[dynamicAR[j]]] = _innerScorer.score();
                    break;
          case TYPENUMBER_FACET_INT:  
                    ints[_arrayIndex[dynamicAR[j]]] = ((TermIntList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(docID));
                    break;
          case TYPENUMBER_FACET_LONG:
                    longs[_arrayIndex[dynamicAR[j]]] = ((TermLongList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(docID));
                    break;
          case TYPENUMBER_FACET_DOUBLE:  
                    doubles[_arrayIndex[dynamicAR[j]]] = ((TermDoubleList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(docID));
                    break;
          case TYPENUMBER_FACET_FLOAT: 
                    floats[_arrayIndex[dynamicAR[j]]] = ((TermFloatList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(docID));
                    break;
          case TYPENUMBER_FACET_SHORT: 
                    shorts[_arrayIndex[dynamicAR[j]]] = ((TermShortList)_termLists[_facetIndex[dynamicAR[j]]]).getPrimitiveValue(_orderArrays[_facetIndex[dynamicAR[j]]].get(docID));
                    break;
          case TYPENUMBER_FACET_STRING:
                    strings[_arrayIndex[dynamicAR[j]]] = ((TermStringList)_termLists[_facetIndex[dynamicAR[j]]]).get(_orderArrays[_facetIndex[dynamicAR[j]]].get(docID));
                    break;
                    
          // multi-facet below;
          case TYPENUMBER_FACET_M_INT:
                    mFacetInts[_mArrayIndex[dynamicAR[j]]].refresh(docID);
                    break;
          case TYPENUMBER_FACET_M_LONG:
                    mFacetLongs[_mArrayIndex[dynamicAR[j]]].refresh(docID);
                    break;
          case  TYPENUMBER_FACET_M_DOUBLE:
                    mFacetDoubles[_mArrayIndex[dynamicAR[j]]].refresh(docID);
                    break;
          case TYPENUMBER_FACET_M_FLOAT:
                    mFacetFloats[_mArrayIndex[dynamicAR[j]]].refresh(docID);
                    break;
          case TYPENUMBER_FACET_M_SHORT:
                    mFacetShorts[_mArrayIndex[dynamicAR[j]]].refresh(docID);
                    break;
          case TYPENUMBER_FACET_M_STRING:
                    mFacetStrings[_mArrayIndex[dynamicAR[j]]].refresh(docID);
                    break;

                    
          // weighted multi-facet below;
          case TYPENUMBER_FACET_WM_INT:
                    ((WeightedMFacetInt)mFacetInts[_mArrayIndex[dynamicAR[j]]]).refresh(docID);
                    break;
          case TYPENUMBER_FACET_WM_LONG:
                    ((WeightedMFacetLong)mFacetLongs[_mArrayIndex[dynamicAR[j]]]).refresh(docID);
                    break;
          case  TYPENUMBER_FACET_WM_DOUBLE:
                    ((WeightedMFacetDouble)mFacetDoubles[_mArrayIndex[dynamicAR[j]]]).refresh(docID);
                    break;
          case TYPENUMBER_FACET_WM_FLOAT:
                    ((WeightedMFacetFloat)mFacetFloats[_mArrayIndex[dynamicAR[j]]]).refresh(docID);
                    break;
          case TYPENUMBER_FACET_WM_SHORT:
                    ((WeightedMFacetShort)mFacetShorts[_mArrayIndex[dynamicAR[j]]]).refresh(docID);
                    break;
          case TYPENUMBER_FACET_WM_STRING:
                    ((WeightedMFacetString)mFacetStrings[_mArrayIndex[dynamicAR[j]]]).refresh(docID);
                    break;
                              
          default: 
                   break;
        }
      }
      
      return _cscorer.score(shorts, ints, longs, floats, doubles, booleans, strings, sets, maps, mFacetInts, mFacetLongs, mFacetFloats, mFacetDoubles, mFacetShorts, mFacetStrings);
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
      
      
      //multi-facet;
      int numMultiFacet = hm_symbol_mfacet.keySet().size();
      final MultiValueFacetDataCache[] mDataCaches = new MultiValueFacetDataCache[numMultiFacet];
      final TermValueList[] mTermLists = new TermValueList[numMultiFacet];
      
      Iterator<String> iter_mfacet = hm_mfacet_index.keySet().iterator();
      while(iter_mfacet.hasNext()){
        String mFacetName = iter_mfacet.next();
        
        // validation;
        Object dataObj = boboReader.getFacetData(mFacetName);
        if ( ! (dataObj instanceof FacetDataCache<?>))
          return createDummyExplain(innerExplain, "Multi-Facet does not exist, return innerExplanation.");
        
        int index = hm_mfacet_index.get(mFacetName);
        mDataCaches[index] = (MultiValueFacetDataCache)(boboReader.getFacetData(mFacetName));
        mTermLists[index] = ((MultiValueFacetDataCache)(boboReader.getFacetData(mFacetName))).valArray;
      }
      
      
      Explanation finalExpl = new Explanation();
      finalExpl.addDetail(innerExplain);
      
      final int paramSize = lls_params.size();
      
      final int[] types = new int[paramSize];
      final int[] facetIndex = new int[paramSize];
      final int[] arrayIndex = new int[paramSize];
      final int[] mFacetIndex = new int[paramSize];  
      final int[] mArrayIndex = new int[paramSize];  
      
      updateArrayIndex(paramSize, types, facetIndex, arrayIndex, mFacetIndex, mArrayIndex);
 
      
      short[] shorts = new short[paramSize];
      int[] ints = new int[paramSize];
      long[] longs = new long[paramSize];
      float[] floats = new float[paramSize];
      double[] doubles = new double[paramSize];
      boolean[] booleans = new boolean[paramSize];
      String[] strings = new String[paramSize];
      Set[] sets = new Set[paramSize];
      Map[] maps = new Map[paramSize];
      
      MFacetInt[] mFacetInts   = new MFacetInt[paramSize];
      MFacetLong[] mFacetLongs = new MFacetLong[paramSize] ;
      MFacetShort[] mFacetShorts = new MFacetShort[paramSize] ;
      MFacetFloat[] mFacetFloats = new MFacetFloat[paramSize];
      MFacetDouble[] mFacetDoubles = new MFacetDouble[paramSize];
      MFacetString[] mFacetStrings = new MFacetString[paramSize];
      
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
        case TYPENUMBER_MAP:
                  maps[arrayIndex[i]] = (Map)hm_var.get(lls_params.get(i));
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
                  
                  
        case TYPENUMBER_FACET_M_INT:
                  mFacetInts[mArrayIndex[i]] =  new MFacetInt(mDataCaches[mFacetIndex[i]]);
                  mFacetInts[mArrayIndex[i]].refresh(doc);
                  break;
        case TYPENUMBER_FACET_M_LONG:
                  mFacetLongs[mArrayIndex[i]] =  new MFacetLong(mDataCaches[mFacetIndex[i]]);
                  mFacetLongs[mArrayIndex[i]].refresh(doc);
                  break;
        case  TYPENUMBER_FACET_M_DOUBLE:
                  mFacetDoubles[mArrayIndex[i]] =  new MFacetDouble(mDataCaches[mFacetIndex[i]]);
                  mFacetDoubles[mArrayIndex[i]].refresh(doc);
                  break;
        case TYPENUMBER_FACET_M_FLOAT:
                  mFacetFloats[mArrayIndex[i]] =  new MFacetFloat(mDataCaches[mFacetIndex[i]]);
                  mFacetFloats[mArrayIndex[i]].refresh(doc);
                  break;
        case TYPENUMBER_FACET_M_SHORT:
                  mFacetShorts[mArrayIndex[i]] =  new MFacetShort(mDataCaches[mFacetIndex[i]]);
                  mFacetShorts[mArrayIndex[i]].refresh(doc);
                  break;
        case TYPENUMBER_FACET_M_STRING:                    
                  mFacetStrings[mArrayIndex[i]] =  new MFacetString(mDataCaches[mFacetIndex[i]]);
                  mFacetStrings[mArrayIndex[i]].refresh(doc);
                  break;  
          
 
        case TYPENUMBER_FACET_WM_INT:
                  mFacetInts[mArrayIndex[i]] =  new WeightedMFacetInt(mDataCaches[mFacetIndex[i]]);
                  ((WeightedMFacetInt)mFacetInts[mArrayIndex[i]]).refresh(doc);
                  break;
        case TYPENUMBER_FACET_WM_LONG:
                  mFacetLongs[mArrayIndex[i]] =  new WeightedMFacetLong(mDataCaches[mFacetIndex[i]]);
                  ((WeightedMFacetLong)mFacetLongs[mArrayIndex[i]]).refresh(doc);
                  break;
        case  TYPENUMBER_FACET_WM_DOUBLE:
                  mFacetDoubles[mArrayIndex[i]] =  new WeightedMFacetDouble(mDataCaches[mFacetIndex[i]]);
                  ((WeightedMFacetDouble)mFacetDoubles[mArrayIndex[i]]).refresh(doc);
                  break;
        case TYPENUMBER_FACET_WM_FLOAT:
                  mFacetFloats[mArrayIndex[i]] =  new WeightedMFacetFloat(mDataCaches[mFacetIndex[i]]);
                  ((WeightedMFacetFloat)mFacetFloats[mArrayIndex[i]]).refresh(doc);
                  break;
        case TYPENUMBER_FACET_WM_SHORT:
                  mFacetShorts[mArrayIndex[i]] =  new WeightedMFacetShort(mDataCaches[mFacetIndex[i]]);
                  ((WeightedMFacetShort)mFacetShorts[mArrayIndex[i]]).refresh(doc);
                  break;
        case TYPENUMBER_FACET_WM_STRING:                    
                  mFacetStrings[mArrayIndex[i]] =  new WeightedMFacetString(mDataCaches[mFacetIndex[i]]);
                  ((WeightedMFacetString)mFacetStrings[mArrayIndex[i]]).refresh(doc);
                  break;                    
                  
        default: 
                 break;
        }
      }
      
      float value = cscorer.score(shorts, ints, longs, floats, doubles, booleans, strings, sets, maps, mFacetInts, mFacetLongs, mFacetFloats, mFacetDoubles, mFacetShorts, mFacetStrings);
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
