package com.senseidb.search.relevance.impl;

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

import com.senseidb.search.relevance.ExternalRelevanceDataStorage;
import com.senseidb.search.req.ErrorType;

import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;

/**

  "relevance": {

      // (1) Model definition part; this json is used to define a model (input variables,
      //     columns/facets, and function parameters and body);

      "model": {
          "variables": {
              "set_int":["c","d"],   // supported hashset types: [set_int, set_float, set_string,
                                     // set_double, set_long]
              "map_int_float":["j"], // supported hashmap: [map_int_float, map_int_double, map_int_*...]
                                     //                    [map_string_int, map_string_float, map_string_*]
              "int":["e","f"],       // supported normal variables: [int, double, float, long, bool, string]
              "long":["g","h"],
              "custom_obj":["krati","big_cache"]   // supported external static big in-memory object (initialized when senseidb starts)
          },

          "facets": {
              "int":["year","age"],  // facet type support: [double, float, int, long, short, string];
              "long":["time"]        // facet variable has the same name as the facet name, and they
                                     // are defined inside this json;
          },

          // (2) Scoring function and function input parameters in Java; A scoring function and its
          //     parameters are the model. A model changes when the function body or signature
          //     changes;

          // Params for the function.  The symbol order matters, and symbols must be those defined
          // above. innerScore MUST be used, otherwise, makes no sense to use the custom relevance;
          // reserved keyword for internal parameters are: "_INNER_SCORE" and "_NOW"

          "function_params":["_INNER_SCORE", "timeVal", "_timeWeight", "_waterworldWeight", "_half_time"],

          // The value string in the following JSONObject is like this (a return statement MUST
          // appear as the last one):
          //
          //    float delta = System.currentTimeMillis() - timeVal;
          //    float t = delta>0 ? delta : 0;
          //    float hour = t/(1000*3600);
          //    float timeScore = (float) Math.exp(-(hour/_half_time));
          //    float waterworldScore = _INNER_SCORE;
          //    float time = timeScore * _timeWeight;
          //    float water = waterworldScore  * _waterworldWeight;
          //    return  (time + water);

          "function":"A LONG JAVA CODE STRING HERE, ONLY AS FUNCTION BODY, NEEDS RETURN STATEMENT."
      }

      // (3) Input values for the model above, if the model requires input values;

      "values": {
          "c":[1996,1997],
          "e":0.98,
          "j":{"1":2.3, "2":3.4, "3":2.9}      // A user input hashmap;
          "jj":{"key":[1,2,3], "value":[2.3, 3.4, 2.9]}      //  It also supports this method to pass in a map. 
      }
  }

  A dummy testing relevance json inside a query request json may look
  like this:

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
                      "function_params":["_INNER_SCORE",
                                         "thisYear",
                                         "year",
                                         "goodYear",
                                         "mileageWeight",
                                         "mileage",
                                         "color",
                                         "yearcolor",
                                         "colorweight",
                                         "category",
                                         "categorycolor"],
                      "function":"  if (tags.contains(coolTag))                         \
                                      return 999999f;                                   \
                                    if (categorycolor.containsKey(category)             \
                                        && categorycolor.get(category).equals(color))   \
                                      return 10000f;                                    \
                                    if (colorweight.containsKey(color))                 \
                                      return 200f + colorweight.getFloat(color);        \
                                    if (yearcolor.containsKey(year) &&                  \
                                        yearcolor.get(year).equals(color))              \
                                      return 200f;                                      \
                                    if (mileageWeight.containsKey(mileage))             \
                                      return 10000+mileageWeight.get(mileage);          \
                                    if (goodYear.contains(year))                        \
                                      return (float)Math.exp(2d);                       \
                                    if (year == thisYear)                               \
                                      return 87f;                                       \
                                    return  _INNER_SCORE;"
                  },

                  "values":{
                       "goodYear":[1996,1997],
                       "thisYear":2001,
                       "mileageWeight":{"11400":777.9, "11000":10.2},
                      "yearcolor":{"1998":"red"},
                      "colorweight":{"red":335.5},
                      "categorycolor":{"compact":"red"},
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

  An advanded usage of weighted multi-facet relevance can be:

  {
      "query": {
          "query_string": {
              "query": "java",
              "relevance": {
                  "model": {
                      "variables": {
                          "string":["skill"]
                       },
                      "facets": {
                          "wmstring":["user_skills"]
                      },
                      "function_params":["_INNER_SCORE",
                                         "user_skills",
                                         "skill"],
                      "function":" int weight = 0;                      \
                                   if (user_skills.hasWeight(skill))    \
                                     weight = user_skills.getWeight();  \
                                   return _INNER_SCORE + weight;"
                  },
                  "values": {
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

*/

public class CompilationHelper
{
  private static Logger logger = Logger.getLogger(CompilationHelper.class);

  private static ClassPool pool = ClassPool.getDefault();
  // White list of safe classes
  private static HashSet<String> hs_safe = new HashSet<String>();

  // Format strings for relevance model parameters
  private static String[] PARAM_FORMAT_STRINGS = new String[]
  {
    /*  0 */ "  int %s = ints[%d];",
    /*  1 */ "  long %s = longs[%d];",
    /*  2 */ "  double %s = doubles[%d];",
    /*  3 */ "  float %s = floats[%d];",
    /*  4 */ "  String %s = strings[%d];",
    /*  5 */ "  short %s = shorts[%d];",
    /*  6 */ "  boolean %s = booleans[%d];",
    /*  7 */ "  it.unimi.dsi.fastutil.ints.IntOpenHashSet %s = (it.unimi.dsi.fastutil.ints.IntOpenHashSet) sets[%d];",
    /*  8 */ "  it.unimi.dsi.fastutil.longs.LongOpenHashSet %s = (it.unimi.dsi.fastutil.longs.LongOpenHashSet) sets[%d];",
    /*  9 */ "  it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet %s = (it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet) sets[%d];",
    /* 10 */ "  it.unimi.dsi.fastutil.floats.FloatOpenHashSet %s = (it.unimi.dsi.fastutil.floats.FloatOpenHashSet) sets[%d];",
    /* 11 */ "  it.unimi.dsi.fastutil.objects.ObjectOpenHashSet %s = (it.unimi.dsi.fastutil.objects.ObjectOpenHashSet) sets[%d];",
    /* 12 */ "  it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap %s = (it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap) maps[%d];",
    /* 13 */ "  it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap %s = (it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap) maps[%d];",
    /* 14 */ "  it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap %s = (it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap) maps[%d];",
    /* 15 */ "  it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap %s = (it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap) maps[%d];",
    /* 16 */ "  it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap %s = (it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap) maps[%d];",
    /* 17 */ "  it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap %s = (it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap) maps[%d];",
    /* 18 */ "  it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap %s = (it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap) maps[%d];",
    /* 19 */ "  it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap %s = (it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap) maps[%d];",
    /* 20 */ "  it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap %s = (it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap) maps[%d];",
    /* 21 */ "  it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap %s = (it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap) maps[%d];",
    /* 22 */ "  com.senseidb.search.relevance.impl.MFacetInt %s = mFacetInts[%d];",
    /* 23 */ "  com.senseidb.search.relevance.impl.MFacetLong %s = mFacetLongs[%d];",
    /* 24 */ "  com.senseidb.search.relevance.impl.MFacetDouble %s = mFacetDoubles[%d];",
    /* 25 */ "  com.senseidb.search.relevance.impl.MFacetFloat %s = mFacetFloats[%d];",
    /* 26 */ "  com.senseidb.search.relevance.impl.MFacetString %s = mFacetStrings[%d];",
    /* 27 */ "  com.senseidb.search.relevance.impl.MFacetShort %s = mFacetShorts[%d];",
    /* 28 */ "  com.senseidb.search.relevance.impl.WeightedMFacetInt %s = (com.senseidb.search.relevance.impl.WeightedMFacetInt) mFacetInts[%d];",
    /* 29 */ "  com.senseidb.search.relevance.impl.WeightedMFacetLong %s = (com.senseidb.search.relevance.impl.WeightedMFacetLong) mFacetLongs[%d];",
    /* 30 */ "  com.senseidb.search.relevance.impl.WeightedMFacetDouble %s = (com.senseidb.search.relevance.impl.WeightedMFacetDouble) mFacetDoubles[%d];",
    /* 31 */ "  com.senseidb.search.relevance.impl.WeightedMFacetFloat %s = (com.senseidb.search.relevance.impl.WeightedMFacetFloat) mFacetFloats[%d];",
    /* 32 */ "  com.senseidb.search.relevance.impl.WeightedMFacetString %s = (com.senseidb.search.relevance.impl.WeightedMFacetString) mFacetStrings[%d];",
    /* 33 */ "  com.senseidb.search.relevance.impl.WeightedMFacetShort %s = (com.senseidb.search.relevance.impl.WeightedMFacetShort) mFacetShorts[%d];",
    /* 34 */ "  %s %s = (%s) objs[%d];"
  };

  // Map of parameter types to int arrays.  For each parameter type, the
  // int array contains two elements: the first one is the index of
  // PARAM_FORMAT_STRINGS for the parameter type, and the second one is
  // the index of input data array for that parameter.
  private static Map<Integer, int[]> PARAM_INIT_MAP = new HashMap<Integer, int[]>();

  private static int TOTAL_INPUT_DATA_ARRAYS = 16;

  private static String EXP_INT_METHOD    = "public double exp(int val) { return Double.longBitsToDouble(((long) (1512775 * val + 1072632447)) << 32); }";
  private static String EXP_DOUBLE_METHOD = "public double exp(double val) { return Double.longBitsToDouble(((long) (1512775 * val + 1072632447)) << 32); }";
  private static String EXP_FLOAT_METHOD  = "public double exp(float val) { return Double.longBitsToDouble(((long) (1512775 * val + 1072632447)) << 32); }";

  private static String SCORE_METHOD_HEADER =
    "public float score(short[] shorts, " +
    "int[] ints, " +
    "long[] longs, " +
    "float[] floats, " +
    "double[] doubles, " +
    "boolean[] booleans, " +
    "String[] strings, " +
    "Set[] sets, " +
    "Map[] maps, " +
    "com.senseidb.search.relevance.impl.MFacetInt[] mFacetInts, " +
    "com.senseidb.search.relevance.impl.MFacetLong[] mFacetLongs, " +
    "com.senseidb.search.relevance.impl.MFacetFloat[] mFacetFloats, " +
    "com.senseidb.search.relevance.impl.MFacetDouble[] mFacetDoubles, " +
    "com.senseidb.search.relevance.impl.MFacetShort[] mFacetShorts, " +
    "com.senseidb.search.relevance.impl.MFacetString[] mFacetStrings, " +
    "java.lang.Object[] objs)";

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
    hs_safe.add("it.unimi.dsi.fastutil.objects.AbstractObject2FloatFunction");


    hs_safe.add("com.senseidb.search.relevance.impl.MFacet");
    hs_safe.add("com.senseidb.search.relevance.impl.MFacetDouble");
    hs_safe.add("com.senseidb.search.relevance.impl.MFacetFloat");
    hs_safe.add("com.senseidb.search.relevance.impl.MFacetInt");
    hs_safe.add("com.senseidb.search.relevance.impl.MFacetLong");
    hs_safe.add("com.senseidb.search.relevance.impl.MFacetShort");
    hs_safe.add("com.senseidb.search.relevance.impl.MFacetString");

    hs_safe.add("com.senseidb.search.relevance.impl.WeightedMFacet");
    hs_safe.add("com.senseidb.search.relevance.impl.WeightedMFacetDouble");
    hs_safe.add("com.senseidb.search.relevance.impl.WeightedMFacetFloat");
    hs_safe.add("com.senseidb.search.relevance.impl.WeightedMFacetInt");
    hs_safe.add("com.senseidb.search.relevance.impl.WeightedMFacetLong");
    hs_safe.add("com.senseidb.search.relevance.impl.WeightedMFacetShort");
    hs_safe.add("com.senseidb.search.relevance.impl.WeightedMFacetString");
    
    hs_safe.add("java.util.Random");

    pool.importPackage("java.util");
    for (String cls: hs_safe)
    {
      pool.importPackage(cls);
    }
    pool.insertClassPath(new ClassClassPath(CompilationHelper.class));

    hs_safe.add("com.senseidb.search.relevance.impl.CustomMathModel");
    hs_safe.add("com.senseidb.search.relevance.impl.CompilationHelper$CustomLoader");

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

    //  Index of input data index for different types:
    //
    //  0  int_index        6  m_int_index       12  boolean_index
    //  1  long_index       7  m_long_index      13  set_index
    //  2  double_index     8  m_double_index    14  map_index
    //  3  float_index      9  m_float_index     15  obj_index
    //  4  string_index    10  m_string_index
    //  5  short_index     11  m_short_index

    // the first int in the following int[] is the index of the string template above (PARAM_FORMAT_STRINGS);
    // the second int in the int[] below is the index of the function parameters;
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_INNER_SCORE,       new int[]{ 3,  3});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_NOW,               new int[]{ 1,  1});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_INT,               new int[]{ 0,  0});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_LONG,              new int[]{ 1,  1});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_DOUBLE,            new int[]{ 2,  2});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FLOAT,             new int[]{ 3,  3});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_STRING,            new int[]{ 4,  4});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_BOOLEAN,           new int[]{ 6, 12});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FACET_INT,         new int[]{ 0,  0});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FACET_LONG,        new int[]{ 1,  1});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FACET_DOUBLE,      new int[]{ 2,  2});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FACET_FLOAT,       new int[]{ 3,  3});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FACET_STRING,      new int[]{ 4,  4});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FACET_SHORT,       new int[]{ 5,  5});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_SET_INT,           new int[]{ 7, 13});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_SET_LONG,          new int[]{ 8, 13});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_SET_DOUBLE,        new int[]{ 9, 13});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_SET_FLOAT,         new int[]{10, 13});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_SET_STRING,        new int[]{11, 13});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_MAP_INT_INT,       new int[]{12, 14});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_MAP_INT_LONG,      new int[]{13, 14});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_MAP_INT_DOUBLE,    new int[]{14, 14});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_MAP_INT_FLOAT,     new int[]{15, 14});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_MAP_INT_STRING,    new int[]{16, 14});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_MAP_STRING_INT,    new int[]{17, 14});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_MAP_STRING_LONG,   new int[]{18, 14});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_MAP_STRING_DOUBLE, new int[]{19, 14});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_MAP_STRING_FLOAT,  new int[]{20, 14});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_MAP_STRING_STRING, new int[]{21, 14});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FACET_M_INT,       new int[]{22,  6});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FACET_M_LONG,      new int[]{23,  7});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FACET_M_DOUBLE,    new int[]{24,  8});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FACET_M_FLOAT,     new int[]{25,  9});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FACET_M_STRING,    new int[]{26, 10});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FACET_M_SHORT,     new int[]{27, 11});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FACET_WM_INT,      new int[]{28,  6});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FACET_WM_LONG,     new int[]{29,  7});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FACET_WM_DOUBLE,   new int[]{30,  8});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FACET_WM_FLOAT,    new int[]{31,  9});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FACET_WM_STRING,   new int[]{32, 10});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FACET_WM_SHORT,    new int[]{33, 11});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_FACET_A_INT,       new int[]{0,   0});
    PARAM_INIT_MAP.put(RelevanceJSONConstants.TYPENUMBER_CUSTOM_OBJ,        new int[]{34, 15});
  }

  private static int MAX_NUM_MODELS  = 10000;
  static HashMap<String, CustomMathModel> hmModels = new HashMap<String, CustomMathModel>();

  public static CustomMathModel createCustomMathScorer(JSONObject jsonModel, DataTable dataTable) throws RelevanceException, JSONException
  {
    CustomMathModel cMathModel = null;

    if(jsonModel == null)
      throw new RelevanceException(ErrorType.JsonParsingError, "No json model is specified.");

    JSONObject jsonVariables = jsonModel.optJSONObject(RelevanceJSONConstants.KW_VARIABLES);
    JSONObject jsonFacets = jsonModel.optJSONObject(RelevanceJSONConstants.KW_FACETS);

    // Process the function body and parameters firstly

    JSONArray jsonFuncParameter = jsonModel.optJSONArray(RelevanceJSONConstants.KW_FUNC_PARAMETERS);
    for(int j=0; j<jsonFuncParameter.length(); j++)
    {
      String paramName = jsonFuncParameter.optString(j);
      dataTable.lls_params.add(paramName);
    }

    dataTable.funcBody = jsonModel.optString(RelevanceJSONConstants.KW_FUNCTION);

    // Process facet variables
    int[] facetIndice = new int[]{0, 0, 0};  // store the facetIndex, facetMultiIndex, and activity engine facet Index;
    Iterator<String> it_facet = jsonFacets.keys();
    while(it_facet.hasNext())
    {
      String facetType = it_facet.next();
      JSONArray facetArray = jsonFacets.getJSONArray(facetType);

     try {
         handleFacetSymbols(facetType, facetArray, facetIndice, dataTable);
     } catch (JSONException e) {
         logger.error("JSON facets are " + jsonFacets);
         throw e;
     }
    }

    // Process other variables
    Iterator<String> it_var = jsonVariables.keys();
    while(it_var.hasNext())
    {
      String type = it_var.next();
      JSONArray varArray = jsonVariables.getJSONArray(type);
      for (int i = 0; i < varArray.length(); ++i)
      {
        String symbol = varArray.getString(i);
        if (symbol.equals(RelevanceJSONConstants.KW_INNER_SCORE) ||
            symbol.equals(RelevanceJSONConstants.KW_NOW) ||
            symbol.equals(RelevanceJSONConstants.KW_RANDOM))
        {
          throw new RelevanceException(ErrorType.JsonParsingError, "Internal variable name, " + symbol + ", is reserved.");
        }

        Integer typeNum = RelevanceJSONConstants.VARIABLE_INFO_MAP.get(type);
        if (typeNum == null)
        {
          throw new RelevanceException(ErrorType.JsonParsingError, "Variable type, " + type + ", is not recognized.");
        }
        dataTable.hm_type.put(symbol, typeNum);
      }
    }

    // Add the _NOW variable
    String symbolNow = RelevanceJSONConstants.KW_NOW;
    long now = System.currentTimeMillis();
    dataTable.hm_var.put(symbolNow, now);
    dataTable.hm_type.put(symbolNow, RelevanceJSONConstants.TYPENUMBER_NOW);

    // Add the _INNER_SCORE variable
    String symbolInnerScore = RelevanceJSONConstants.KW_INNER_SCORE;
    dataTable.hm_var.put(symbolInnerScore, symbolInnerScore);
    dataTable.hm_type.put(symbolInnerScore, RelevanceJSONConstants.TYPENUMBER_INNER_SCORE);

    if(dataTable.funcBody == null || dataTable.funcBody.length()==0)
      throw new RelevanceException(ErrorType.JsonParsingError, "No function body found.");

    if(dataTable.funcBody.indexOf("return ")==-1)
      throw new RelevanceException(ErrorType.JsonParsingError, "No return statement in the function body.");


    // Check if all the parameters have been defined
    for(int i=0; i< dataTable.lls_params.size(); i++)
    {
      String symbol = dataTable.lls_params.get(i);
      if( !dataTable.hm_type.containsKey(symbol))
        throw new RelevanceException(ErrorType.JsonParsingError, "function parameter: " + symbol + " was not defined.");

      Integer typeNum = dataTable.hm_type.get(symbol);
      if (typeNum >= RelevanceJSONConstants.TYPENUMBER_FACET_INT && typeNum <= RelevanceJSONConstants.TYPENUMBER_FACET_WM_STRING)
      {
        if( (!dataTable.hm_symbol_facet.containsKey(symbol)) && (!dataTable.hm_symbol_mfacet.containsKey(symbol)))
          throw new RelevanceException(ErrorType.JsonParsingError, "function parameter: " + symbol + " was not defined.");
      }
    }

    dataTable.lls_params = filterParameters(dataTable);

    // Compile the math model below
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
      synchronized(CompilationHelper.class)
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
          ci = CompilationHelper.pool.get("com.senseidb.search.relevance.impl.CustomMathModel");
        }
        catch (NotFoundException e)
        {
          logger.info(e.getMessage());
          throw new RelevanceException(e);
        }

        ch.addInterface(ci);
        String functionString = makeFuncString(dataTable);

        addStaticFacilityFields(ch);
        addStaticFacilityMethods(ch);

        CtMethod m;
        try
        {
          m = CtNewMethod.make(functionString, ch);
          ch.addMethod(m);
        }
        catch (CannotCompileException e)
        {
          logger.info(e.getMessage());
          throw new RelevanceException(ErrorType.JsonCompilationError, e.getMessage(), e);
        }

        Class h;
        try
        {
          h = CompilationHelper.pool.toClass(ch, new CompilationHelper.CustomLoader(CompilationHelper.class.getClassLoader(), className));
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
            throw new RelevanceException(ErrorType.JsonCompilationError, "Compilation error of json relevance model.", e);
          }
        }

        try
        {
          cMathModel = (CustomMathModel)h.newInstance();
        }
        catch (InstantiationException e)
        {
          logger.info(e.getMessage());
          throw new RelevanceException(ErrorType.JsonCompilationError, "Instantiation exception of relevance object.", e);
        }
        catch (IllegalAccessException e)
        {
          logger.info(e.getMessage());
          throw new RelevanceException(ErrorType.JsonCompilationError, "Instantiation exception of relevance object; Illegal access exception", e);
        }

        if(hmModels.size() > MAX_NUM_MODELS)
          hmModels = new HashMap<String, CustomMathModel>();

        hmModels.put(className, cMathModel);
        logger.info("get math model by compilation:"+ className);
        return cMathModel;
      }
    }
  }

  public static void initializeValues(JSONObject jsonValues, DataTable dataTable) throws JSONException
  {
    HashMap<String, Integer> hm_type = dataTable.hm_type;
    Iterator it = hm_type.keySet().iterator();
    while(it.hasNext())
    {
      String symbol = (String)it.next();
      Integer typeNum = dataTable.hm_type.get(symbol);

      if (typeNum == RelevanceJSONConstants.TYPENUMBER_INNER_SCORE ||
          typeNum == RelevanceJSONConstants.TYPENUMBER_NOW )
        continue;

      if (typeNum >= RelevanceJSONConstants.TYPENUMBER_SET_INT &&
          typeNum <= RelevanceJSONConstants.TYPENUMBER_SET_STRING)
      {
        Set hs = null;
        JSONArray values = jsonValues.optJSONArray(symbol);
        if (values == null)
          throw new JSONException("Variable "+ symbol + " does not have value.");

        switch (typeNum)
        {
        case RelevanceJSONConstants.TYPENUMBER_SET_INT:
          hs = new IntOpenHashSet();
          for (int k = 0; k < values.length(); k++)
          {
            hs.add(values.getInt(k));
          }
          break;
        case RelevanceJSONConstants.TYPENUMBER_SET_DOUBLE:
          hs = new DoubleOpenHashSet();
          for (int k = 0; k < values.length(); k++)
          {
            hs.add(values.getDouble(k));
          }
          break;
        case RelevanceJSONConstants.TYPENUMBER_SET_FLOAT:
          hs = new FloatOpenHashSet();
          for (int k = 0; k < values.length(); k++)
          {
            hs.add((float) values.getDouble(k));
          }
          break;
        case RelevanceJSONConstants.TYPENUMBER_SET_LONG:
          hs = new LongOpenHashSet();
          for (int k = 0; k < values.length(); k++)
          {
            hs.add(values.getLong(k));
          }
          break;
        case RelevanceJSONConstants.TYPENUMBER_SET_STRING:
          hs = new ObjectOpenHashSet();
          for (int k = 0; k < values.length(); k++)
          {
            hs.add(values.getString(k));
          }
          break;
        }
        dataTable.hm_var.put(symbol, hs);
      }
      else if (typeNum >= RelevanceJSONConstants.TYPENUMBER_MAP_INT_INT &&
               typeNum <= RelevanceJSONConstants.TYPENUMBER_MAP_STRING_STRING)
      {
        JSONObject values = jsonValues.optJSONObject(symbol);
        if (values == null)
          throw new JSONException("Variable "+ symbol + " does not have value.");

        JSONArray keysList = values.names();
        int keySize = keysList == null ? 0 : keysList.length();
        
        // denote if the map is represented in a way of combination of key jsonarray and value jsonarray;
        boolean isKeyValue = isKeyValueArrayMethod(values);
        JSONArray keysArrayList = null, valuesArrayList = null;
        int keyArraySize, valueArraySize;
        if(isKeyValue)
        {
          keysArrayList = values.optJSONArray(RelevanceJSONConstants.KW_KEY);
          valuesArrayList = values.optJSONArray(RelevanceJSONConstants.KW_VALUE);
          
          if (keysArrayList == null)
            throw new JSONException("Variable " + symbol + " is a map, but does not have a key list.");

          if (valuesArrayList == null)
            throw new JSONException("Variable " + symbol + "is a map, but does not have a value list.");

          keyArraySize = keysArrayList.length();
          valueArraySize = valuesArrayList.length();
          if (keyArraySize != valueArraySize)
            throw new JSONException("Variable " + symbol + ": key size is different from value size, can not convert to a map." );
          
          keySize = keysArrayList.length();
        }

        Map hm = null;
        switch (typeNum)
        {
        case RelevanceJSONConstants.TYPENUMBER_MAP_INT_INT:
          hm = new Int2IntOpenHashMap();
          for (int j = 0; j < keySize; j++)
          {
            if(isKeyValue)
              ((Int2IntOpenHashMap) hm).put(keysArrayList.getInt(j), valuesArrayList.getInt(j));
            else
              ((Int2IntOpenHashMap) hm).put(keysList.getInt(j), values.getInt(keysList.getString(j)));
          }
          break;
        case RelevanceJSONConstants.TYPENUMBER_MAP_INT_DOUBLE:
          hm = new Int2DoubleOpenHashMap();
          for (int j = 0; j < keySize; j++)
          {
            if(isKeyValue)
              ((Int2DoubleOpenHashMap) hm).put(keysArrayList.getInt(j), valuesArrayList.getDouble(j));
            else
              ((Int2DoubleOpenHashMap) hm).put(keysList.getInt(j), values.getDouble(keysList.getString(j)));
          }
          break;
        case RelevanceJSONConstants.TYPENUMBER_MAP_INT_FLOAT:
          hm = new Int2FloatOpenHashMap();
          for (int j = 0; j < keySize; j++)
          {
            if(isKeyValue)
              ((Int2FloatOpenHashMap) hm).put(keysArrayList.getInt(j), (float) valuesArrayList.getDouble(j));
            else
              ((Int2FloatOpenHashMap) hm).put(keysList.getInt(j), (float) values.getDouble(keysList.getString(j)));
          }
          break;
        case RelevanceJSONConstants.TYPENUMBER_MAP_INT_LONG:
          hm = new Int2LongOpenHashMap();
          for (int j = 0; j < keySize; j++)
          {
            if(isKeyValue)
              ((Int2LongOpenHashMap) hm).put(keysArrayList.getInt(j), Long.parseLong(valuesArrayList.getString(j)));
            else
              ((Int2LongOpenHashMap) hm).put(keysList.getInt(j), Long.parseLong(values.getString(keysList.getString(j))));
          }
          break;
        case RelevanceJSONConstants.TYPENUMBER_MAP_INT_STRING:
          hm = new Int2ObjectOpenHashMap();
          for (int j = 0; j < keySize; j++)
          {
            if(isKeyValue)
              ((Int2ObjectOpenHashMap) hm).put(keysArrayList.getInt(j), valuesArrayList.getString(j));
            else
              ((Int2ObjectOpenHashMap) hm).put(keysList.getInt(j), values.getString(keysList.getString(j)));
          }
          break;
        case RelevanceJSONConstants.TYPENUMBER_MAP_STRING_INT:
          hm = new Object2IntOpenHashMap();
          for (int j = 0; j < keySize; j++)
          {
            if(isKeyValue)
              ((Object2IntOpenHashMap) hm).put(keysArrayList.getString(j), valuesArrayList.getInt(j));
            else
            {
              String key = keysList.getString(j);
              ((Object2IntOpenHashMap) hm).put(key, values.getInt(key));
            }
          }
          break;
        case RelevanceJSONConstants.TYPENUMBER_MAP_STRING_DOUBLE:
          hm = new Object2DoubleOpenHashMap();
          for (int j = 0; j < keySize; j++)
          {
            if(isKeyValue)
              ((Object2DoubleOpenHashMap) hm).put(keysArrayList.getString(j), valuesArrayList.getDouble(j));
            else
            {
            String key = keysList.getString(j);
            ((Object2DoubleOpenHashMap) hm).put(key, values.getDouble(key));
            }
          }
          break;
        case RelevanceJSONConstants.TYPENUMBER_MAP_STRING_FLOAT:
          hm = new Object2FloatOpenHashMap();
          for (int j = 0; j < keySize; j++)
          {
            if(isKeyValue)
              ((Object2FloatOpenHashMap) hm).put(keysArrayList.getString(j), (float) valuesArrayList.getDouble(j));
            else
            {
              String key = keysList.getString(j);
              ((Object2FloatOpenHashMap) hm).put(key, (float) values.getDouble(key));
            }
          }
          break;
        case RelevanceJSONConstants.TYPENUMBER_MAP_STRING_LONG:
          hm = new Object2LongOpenHashMap();
          for (int j = 0; j < keySize; j++)
          {
            if(isKeyValue)
              ((Object2LongOpenHashMap) hm).put(keysArrayList.getString(j), Long.parseLong(valuesArrayList.getString(j)));
            else
            {
              String key = keysList.getString(j);
              ((Object2LongOpenHashMap) hm).put(key, Long.parseLong(values.getString(keysList.getString(j))));
            }
          }
          break;
        case RelevanceJSONConstants.TYPENUMBER_MAP_STRING_STRING:
          hm = new Object2ObjectOpenHashMap();
          for (int j = 0; j < keySize; j++)
          {
            if(isKeyValue)
              ((Object2ObjectOpenHashMap) hm).put(keysArrayList.getString(j), valuesArrayList.getString(j));
            else
            {
              String key = keysList.getString(j);
              ((Object2ObjectOpenHashMap) hm).put(key, values.getString(key));
            }
          }
          break;
        }
        dataTable.hm_var.put(symbol, hm);
      }
      else if (typeNum >= RelevanceJSONConstants.TYPENUMBER_INT &&
               typeNum <= RelevanceJSONConstants.TYPENUMBER_STRING)
      {
        if (!jsonValues.has(symbol))
          throw new JSONException("Symbol " + symbol + " was not assigned with a value." );

        switch (typeNum)
        {
        case RelevanceJSONConstants.TYPENUMBER_INT:
          dataTable.hm_var.put(symbol, jsonValues.getInt(symbol));
          break;
        case RelevanceJSONConstants.TYPENUMBER_DOUBLE:
          dataTable.hm_var.put(symbol, jsonValues.getDouble(symbol));
          break;
        case RelevanceJSONConstants.TYPENUMBER_FLOAT:
          dataTable.hm_var.put(symbol, (float) jsonValues.getDouble(symbol));
          break;
        case RelevanceJSONConstants.TYPENUMBER_LONG:
          dataTable.hm_var.put(symbol, Long.parseLong(jsonValues.getString(symbol)));
          break;
        case RelevanceJSONConstants.TYPENUMBER_STRING:
          dataTable.hm_var.put(symbol, jsonValues.getString(symbol));
          break;
        case RelevanceJSONConstants.TYPENUMBER_BOOLEAN:
          dataTable.hm_var.put(symbol, jsonValues.getBoolean(symbol));
          break;
        }
      }
      else if (typeNum == RelevanceJSONConstants.TYPENUMBER_CUSTOM_OBJ)
      {
        Object obj = ExternalRelevanceDataStorage.getObj(symbol); 
        if(obj != null)
          dataTable.hm_var.put(symbol, obj);
        else
          throw new JSONException("function parameter: " + symbol + " can not be initialized as a custom Object.");
      }
    }

    // Check if all the parameters have been initialized
    for(int i=0; i< dataTable.lls_params.size(); i++)
    {
      String symbol = dataTable.lls_params.get(i);
      Integer typeNum = dataTable.hm_type.get(symbol);
      if (typeNum < RelevanceJSONConstants.TYPENUMBER_FACET_INT ||
          typeNum > RelevanceJSONConstants.TYPENUMBER_FACET_A_INT)
      {
        if(!dataTable.hm_var.containsKey(symbol))
          throw new JSONException("function parameter: " + symbol + " was not initialized.");
      }
    }
  } // End of initializeValues()

  /**
   * check if in the JSON values part the map variable is represented in a json map way or two key and value json array;
   *       "JsonMapway":{"1":2.3, "2":3.4, "3":2.9}      // A user input hashmap;
   *       "KeyValueJsonArrayPairWay":{"key":[1,2,3], "value":[2.3, 3.4, 2.9]}      //  It also supports this method to pass in a map. 
   * @param values
   * @return boolean
   */
  private static boolean isKeyValueArrayMethod(JSONObject mapJSON)
  {
    if(mapJSON.has(RelevanceJSONConstants.KW_KEY) && mapJSON.has(RelevanceJSONConstants.KW_VALUE))
    {
      JSONArray keysList = mapJSON.optJSONArray(RelevanceJSONConstants.KW_KEY);
      JSONArray valuesList = mapJSON.optJSONArray(RelevanceJSONConstants.KW_VALUE);
      if(keysList != null && valuesList != null)
        return true;
    }
    return false;
  }

  private static void addStaticFacilityFields(CtClass ch) throws JSONException
  {
    // add a random field in the object;
    CtField f;
    try
    {
      f = CtField.make("public static java.util.Random _RANDOM = new java.util.Random();", ch);
      ch.addField(f);
    }
    catch (CannotCompileException e)
    {
      logger.info(e.getMessage());
      throw new JSONException(e);
    }
  }
  
  private static void addStaticFacilityMethods(CtClass ch) throws JSONException
  {
    addMethod(EXP_INT_METHOD, ch);
    addMethod(EXP_DOUBLE_METHOD, ch);
    addMethod(EXP_FLOAT_METHOD, ch);
  }

  private static void addMethod(String expStr, CtClass ch) throws JSONException
  {
    CtMethod m_exp;
    try
    {
      m_exp = CtNewMethod.make(expStr, ch);
      ch.addMethod(m_exp);
    }
    catch (CannotCompileException e)
    {
      logger.info(e.getMessage());
      throw new JSONException(e);
    }
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

  private static <T, V> String mkString(Map<T, V> map) {
    StringBuffer sb = new StringBuffer().append("{");

    int count = 0;
    for(Map.Entry<T, V> entry : map.entrySet())
    {
      if(count++ != 0)
          sb.append(", ");

      sb.append(entry.getKey()).append(": ").append(entry.getValue());
    }

    return sb.append("}").toString();
  }

    private static <T> String mkString(Set<T> set) {
        StringBuffer sb = new StringBuffer().append("[");

        int count = 0;
        for(T elem : set)
        {
            if(count++ != 0)
                sb.append(", ");

            sb.append(elem);
        }

        return sb.append("]").toString();
    }

  private static void handleFacetSymbols(String facetType,
                                         JSONArray facetArray,
                                         int[] facetIndice,
                                         DataTable dataTable)
    throws JSONException
  {
    Integer[] facetInfo = RelevanceJSONConstants.FACET_INFO_MAP.get(facetType);
    if (facetInfo == null)
    {
       String errorString = String.format("Wrong facet type in facet variable definition json: %s. Map contents are %s. Facet array is %s.",
         facetType, mkString(RelevanceJSONConstants.FACET_INFO_MAP), facetArray);


       throw new JSONException(errorString);
    }

    Integer type = facetInfo[0];

    for(int i=0; i < facetArray.length(); i++)
    {
      String facetName = facetArray.getString(i);
      String symbol = facetName;

      if(dataTable.hm_symbol_facet.containsKey(symbol) || dataTable.hm_symbol_mfacet.containsKey(symbol) || dataTable.hm_symbol_afacet.containsKey(symbol))
        throw new JSONException("facet Symbol "+ symbol + " already defined." );

      if(dataTable.hm_facet_index.containsKey(facetName) || dataTable.hm_mfacet_index.containsKey(facetName) || dataTable.hm_afacet_index.containsKey(facetName))
        throw new JSONException("facet name "+ facetName + " already assigned to a symbol." );

      if (facetInfo[1] == 0)
      {
        // This facet is a normal facet;
        dataTable.hm_symbol_facet.put(symbol, facetName);
        dataTable.hm_facet_index.put(facetName, facetIndice[0]);
        facetIndice[0] = facetIndice[0]+1;
      }
      else if (facetInfo[1] == 1)
      { 
        // This is a multi-value facet;
        dataTable.hm_symbol_mfacet.put(symbol, facetName);
        dataTable.hm_mfacet_index.put(facetName, facetIndice[1]);
        facetIndice[1] = facetIndice[1]+1;
      }
      else if (facetInfo[1] == 2)
      {
        // This is an activity engine facet;
        dataTable.hm_symbol_afacet.put(symbol, facetName);
        dataTable.hm_afacet_index.put(facetName, facetIndice[2]);
        facetIndice[2] = facetIndice[2]+1;
      }

      dataTable.hm_type.put(symbol, type);
    }
  }

  private static String makeFuncString(DataTable dataTable) throws JSONException
  {
    int[] paramIndices = new int[TOTAL_INPUT_DATA_ARRAYS];
    for (int i = 0; i < TOTAL_INPUT_DATA_ARRAYS; i++)
    {
      paramIndices[i] = 0;
    }

    StringBuffer sb = new StringBuffer();
    sb.append(SCORE_METHOD_HEADER).append(" {");

    dataTable.useInnerScore = false;  // set using innerscore to false at the beginning; once we see innerscore is used in the function below, it will be set to true;

    for(int i=0; i< dataTable.lls_params.size();i++)
    {
      String paramName = dataTable.lls_params.get(i);

      if(!dataTable.hm_type.containsKey(paramName) || (dataTable.hm_type.get(paramName) == null))
        throw new JSONException("function parameter " + paramName + " is not defined.");

      Integer paramType = dataTable.hm_type.get(paramName);
      int[] paramInfo = PARAM_INIT_MAP.get(paramType);
      if(paramType.intValue() == RelevanceJSONConstants.TYPENUMBER_CUSTOM_OBJ)
      {
        String className = ExternalRelevanceDataStorage.getObjClsName(paramName);
        if(className == null)
          throw new JSONException("Custom external object " + paramName + " is not found.");
        
        String className2 = className.replace('$', '.');
        
        hs_safe.add(className);
        pool.importPackage(className);
        
        sb.append(String.format(PARAM_FORMAT_STRINGS[paramInfo[0]], className2, paramName, className2, paramIndices[paramInfo[1]]++));
      }
      else
        sb.append(String.format(PARAM_FORMAT_STRINGS[paramInfo[0]], paramName, paramIndices[paramInfo[1]]++));
      if (paramType == RelevanceJSONConstants.TYPENUMBER_INNER_SCORE)
      {
        dataTable.useInnerScore = true;
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
      else {
        String message = String.format("Unable to load class %s. Safe classes are %s", name, mkString(hs_safe));
        throw new ClassNotFoundException(message);
      }
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
    public HashMap<String, Integer> hm_type;
    public HashMap<String, String> hm_symbol_facet;
    public HashMap<String, Integer> hm_facet_index;
    public HashMap<String, String> hm_symbol_mfacet;  //multi-facet
    public HashMap<String, Integer> hm_mfacet_index; //multi-facet
    public HashMap<String, String> hm_symbol_afacet;  //activity-facet
    public HashMap<String, Integer> hm_afacet_index; //activity-facet
    

    public LinkedList<String> lls_params;
    public String funcBody = null;
    public String classIDString = null;
    public boolean useInnerScore = true;  // by default will calculate innerscore value, set this to false will ignore inner score to save time;

    public DataTable(){
      hm_var = new HashMap<String, Object>();
      hm_type = new HashMap<String, Integer>();
      hm_symbol_facet = new HashMap<String, String>();
      hm_facet_index = new HashMap<String, Integer>();
      hm_symbol_mfacet = new HashMap<String, String>();  //multi-facet
      hm_mfacet_index = new HashMap<String, Integer>(); //multi-facet
      hm_symbol_afacet = new HashMap<String, String>();  //multi-facet
      hm_afacet_index = new HashMap<String, Integer>(); //multi-facet
      lls_params = new LinkedList<String>();
      useInnerScore = true;
    }
  }
}
