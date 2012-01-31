package com.senseidb.search.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
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

  // container types:
  private final String                 TYPE_SET          = "SET";

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

    pool.importPackage("it.unimi.dsi.fastutil.ints.*");
    pool.importPackage("it.unimi.dsi.fastutil.longs.*");
    pool.importPackage("it.unimi.dsi.fastutil.shorts.*");
    pool.importPackage("it.unimi.dsi.fastutil.booleans.*");
    pool.importPackage("it.unimi.dsi.fastutil.bytes.*");
    pool.importPackage("it.unimi.dsi.fastutil.chars.*");
    pool.importPackage("it.unimi.dsi.fastutil.doubles.*");
    pool.importPackage("it.unimi.dsi.fastutil.floats.*");
    pool.importPackage("it.unimi.dsi.fastutil.objects.*");
  }
  
  static HashMap<String, CustomScorer> hmModels = new HashMap<String, CustomScorer>();
  
//  "relevance":[
//               
//               // sequence of statements in a score adjusting function below, statement order matters;
//               // the statement order should be exactly the same as the Java code is executing.
//         
//         
//                   // (1) Variable assignment statements;
//                   //     It may contain set, int, float, double, long, boolean,
//         
//                        // ------  [a] static variables below;
//         
//                   // var_set_int, var_set_string, var_set_double, var_set_long supported now.
//         
//                   {"var_set_int":{"c":[1996, 1997], "d":[1998]}},
//         
//                   {"var_double":{"e":0.98}},
//         
//                   {"var_int":{"g":1996}},
//  
//                   {"var_bool":{"f":true, "h":false}},
//         
//                        // -----  [b] runtime variables: statements here may be (not always) evaluated at runtime;
//         
//                   {"var_constant_long":{"now":"_NOW"}},
//         
//                   {"var_constant_float":{"innerScore":"_INNER_SCORE"}},
//         
//                   {"var_facet_int":{"f":"year"}},  //facet type support: double, float, int, long, short, string;
//         
//         
//         
//                   // (2) scoring function and function input parameters in Java;
//                   //     A scoring function is a model. A model changes when the function body or signature changes;
//                   
//                   {"function_params":["innerScore", "timeVal", "_timeWeight", "_waterworldWeight", "_half_time"]},    //  params for the function above, optional. Symbol order matters, and symbols must be those defined above. innerScore MUST be used, otherwise, makes no sense to use the custom relevance;           
//
//                   // the value string in the following JSONObject is like this (a return statement MUST appear as the last one):
//                         
//                      //    float delta = System.currentTimeMillis() - timeVal;
//                      //    float t = delta>0 ? delta : 0;
//                      //    float hour = t/(1000*3600);
//                      //    float timeScore = (float) Math.exp(-(hour/_half_time));
//                      //    float waterworldScore = innerScore;
//                      //    float time = timeScore * _timeWeight;
//                      //    float water = waterworldScore  * _waterworldWeight;
//                      //    return  (time + water);
//                      
//                   {"function":" A LONG JAVA CODE STRING HERE, ONLY AS FUNCTION BODY"}
//            ]
  
  
  
  
  /* A dummy testing relevance json:
   * 
   * 
   *               "relevance":[
                   {"var_set_int":{"c":[1996, 1997], "d":[1998]}},
                   {"var_double":{"e":0.98}},
                   {"var_int":{"g":1996}},
                   {"var_bool":{"f":true, "h":false}},
                   {"var_constant_long":{"now":"_NOW"}},
                   {"var_constant_float":{"innerScore":"_INNER_SCORE"}},
                   {"var_facet_int":{"f":"color"}},
                   {"function_params":["innerScore"]},              
                   {"function":" return 2f;"}
   * 
   * */
  
  public RelevanceQuery(Query query, JSONArray relevance) throws JSONException
  {
    super(query);
    _query = query;
    preprocess(relevance);
  }
 



  private void preprocess(JSONArray relevance) throws JSONException
  {
    for(int i=0; i< relevance.length(); i++)
    {
      JSONObject stat = relevance.optJSONObject(i);
      Iterator<String> iter = stat.keys();
      if (!iter.hasNext())
        throw new IllegalArgumentException("statement type not specified in relevance query: " + stat);

      String type = iter.next();
      
      // var_set_int, var_set_string, var_set_double, var_set_long
      // {"var_set_int":{"c":[1996, 1997], "d":[1998]}},
      if("var_set_int".equals(type) || "var_set_double".equals(type) || "var_set_long".equals(type) || "var_set_string".equals(type))
      {
        JSONObject sets = stat.optJSONObject(type);
        Iterator<String> iter_symbol = sets.keys();
        while(iter_symbol.hasNext())
        {
          String symbol = iter_symbol.next();
          HashSet hs = new HashSet();
          JSONArray values = sets.getJSONArray(symbol);
          for (int k =0; k < values.length(); k++){
            if("var_set_int".equals(type))
              hs.add(values.getInt(k));
            else if ("var_set_double".equals(type))
              hs.add(values.getDouble(k));
            else if ("var_set_long".equals(type))
              hs.add(values.getLong(k));
            else if ("var_set_string".equals(type))
              hs.add(values.getString(k));
          }
          if(hm_var.containsKey(symbol))
            throw new JSONException("Symbol "+ symbol + " already defined." );
          hm_var.put(symbol, hs);
          hm_type.put(symbol, TYPE_SET);
        }
      }
      
      // var_int, var_string, var_double, var_long
      // {"var_double":{"e":0.98}},
      else if("var_int".equals(type) || "var_double".equals(type) || "var_long".equals(type) || "var_float".equals(type) || "var_string".equals(type))
      {
        JSONObject sets = stat.optJSONObject(type);
        Iterator<String> iter_symbol = sets.keys();

        while(iter_symbol.hasNext())
        {
          String symbol = iter_symbol.next();

          if(hm_var.containsKey(symbol))
            throw new JSONException("Symbol "+ symbol + " already defined." );
          
          if("var_int".equals(type))
          {
            hm_var.put(symbol, sets.getInt(symbol));
            hm_type.put(symbol, TYPE_INT);
          }
          else if ("var_double".equals(type))
          {
            hm_var.put(symbol, sets.getDouble(symbol));
            hm_type.put(symbol, TYPE_DOUBLE);
          }
          else if ("var_float".equals(type))
          {
            hm_var.put(symbol, ((float)sets.getDouble(symbol)));
            hm_type.put(symbol, TYPE_FLOAT);
          }
          else if ("var_long".equals(type))
          {
            hm_var.put(symbol, sets.getLong(symbol));
            hm_type.put(symbol, TYPE_LONG);
          }
          else if ("var_string".equals(type))
          {
            hm_var.put(symbol, sets.getString(symbol));
            hm_type.put(symbol, TYPE_STRING);
          }
        }
      }
      
      // var_bool
      else if("var_bool".equals(type))
      {
        JSONObject set = stat.optJSONObject(type);
        Iterator<String> iterSymbol = set.keys();
        while(iterSymbol.hasNext())
        {
          String symbol = iterSymbol.next();
          Boolean value = set.optBoolean(symbol);
          if(hm_var.containsKey(symbol))
            throw new JSONException("Symbol "+ symbol + " already defined." );
          hm_var.put(symbol, value);
          hm_type.put(symbol, TYPE_BOOLEAN);
        }
      }
      
      // now
      // {"var_constant_long":{"now":"_NOW"}},
      else if("var_constant_long".equals(type))
      {
        JSONObject set = stat.optJSONObject(type);
        Iterator<String> iterSymbol = set.keys();
        while(iterSymbol.hasNext())
        {
          String symbol = iterSymbol.next();
          String value = set.optString(symbol);
          if("_NOW".equals(value))
          {
            long now = System.currentTimeMillis();
            if(hm_var.containsKey(symbol))
              throw new JSONException("Symbol "+ symbol + " already defined." );
            hm_var.put(symbol, now);
            hm_type.put(symbol, TYPE_LONG);
          }
        }
      }
      
      // innerscore;
      //  {"var_constant_float":{"innerScore":"_INNER_SCORE"}},
      else if("var_constant_float".equals(type))
      {
        JSONObject set = stat.optJSONObject(type);
        Iterator<String> iterSymbol = set.keys();
        while(iterSymbol.hasNext())
        {
          String symbol = iterSymbol.next();
          String value = set.optString(symbol);
          if("_INNER_SCORE".equals(value))
          {
            if(hm_var.containsKey(symbol))
              throw new JSONException("Symbol "+ symbol + " already defined." );
            hm_var.put(symbol, "_innerScore");
            hm_type.put(symbol, TYPE_INNER_SCORE);
          }
        }
      }
      
      // var_facet_int, var_facet_string, var_facet_double, var_facet_long
      // {"var_facet_int":{"f":"year"}},
      else if("var_facet_int".equals(type)) 
      {
        JSONObject set = stat.optJSONObject(type);
        handleFacetSymbols(set, TYPE_FACET_INT);
      }
      else if("var_facet_short".equals(type)) 
      {
        JSONObject set = stat.optJSONObject(type);
        handleFacetSymbols(set, TYPE_FACET_SHORT);
      }
      else if("var_facet_double".equals(type))
      {
        JSONObject set = stat.optJSONObject(type);
        handleFacetSymbols(set, TYPE_FACET_DOUBLE);
      }
      else if("var_facet_float".equals(type))
      {
        JSONObject set = stat.optJSONObject(type);
        handleFacetSymbols(set, TYPE_FACET_FLOAT);
      }
      else if("var_facet_long".equals(type))
      {
        JSONObject set = stat.optJSONObject(type);
        handleFacetSymbols(set, TYPE_FACET_LONG);
      }
      else if("var_facet_string".equals(type))
      {
        JSONObject set = stat.optJSONObject(type);
        handleFacetSymbols(set, TYPE_FACET_STRING);
      }

      
      //function parameters;
      if("function_params".equals(type))
      {
        JSONArray sets = stat.optJSONArray(type);
        for(int j=0; j<sets.length(); j++)
        {
          String paramName = sets.optString(j);
          lls_params.add(paramName);
        }
      }

      //function body;
      if("function".equals(type))
      {
        funcBody = stat.optString("function").trim();
      }
      
    } // end of all the statements;
    
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
          h = ch.toClass(RelevanceQuery.class.getClassLoader());
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



  private void handleFacetSymbols(JSONObject set, String type) throws JSONException
  {
    Iterator<String> iterSymbol = set.keys();
    while(iterSymbol.hasNext())
    {
      String symbol = iterSymbol.next();
      String facetName = set.optString(symbol);

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
      else if(hm_type.get(paramName).equals(TYPE_SET))
      {
        sb.append(" Set " + paramName + " = sets["+ set_index +"]; ");
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
      
      final int[] types = new int[paramSize];
      final int[] facetIndex = new int[paramSize];
      final int[] arrayIndex = new int[paramSize];
      
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
        else if (type.equals(TYPE_SET))
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
      
      shorts = new short[_paramSize];
      ints = new int[_paramSize];
      longs = new long[_paramSize];
      floats = new float[_paramSize];
      doubles = new double[_paramSize];
      booleans = new boolean[_paramSize];
      strings = new String[_paramSize];
      sets = new Set[_paramSize];
      
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
        default: 
                 break;
        }
      }
      
    }
    @Override
    public float score() throws IOException {
      
      for(int i=0; i< _paramSize; i++)
      {
        
        // only when the parameter is inner score variable or facet variable, we need to update the score function input parameter arrays; 
        switch (_types[i]) {
          case TYPENUMBER_INNER_SCORE:  
                    floats[_arrayIndex[i]] = _innerScorer.score();
                    break;
          case TYPENUMBER_FACET_INT:  
                    ints[_arrayIndex[i]] = ((TermIntList)_termLists[_facetIndex[i]]).getPrimitiveValue(_orderArrays[_facetIndex[i]].get(_innerScorer.docID()));
                    break;
          case TYPENUMBER_FACET_LONG:
                    longs[_arrayIndex[i]] = ((TermLongList)_termLists[_facetIndex[i]]).getPrimitiveValue(_orderArrays[_facetIndex[i]].get(_innerScorer.docID()));
                    break;
          case TYPENUMBER_FACET_DOUBLE:  
                    doubles[_arrayIndex[i]] = ((TermDoubleList)_termLists[_facetIndex[i]]).getPrimitiveValue(_orderArrays[_facetIndex[i]].get(_innerScorer.docID()));
                    break;
          case TYPENUMBER_FACET_FLOAT: 
                    floats[_arrayIndex[i]] = ((TermFloatList)_termLists[_facetIndex[i]]).getPrimitiveValue(_orderArrays[_facetIndex[i]].get(_innerScorer.docID()));
                    break;
          case TYPENUMBER_FACET_SHORT: 
                    shorts[_arrayIndex[i]] = ((TermShortList)_termLists[_facetIndex[i]]).getPrimitiveValue(_orderArrays[_facetIndex[i]].get(_innerScorer.docID()));
                    break;
          case TYPENUMBER_FACET_STRING:
                    strings[_arrayIndex[i]] = ((TermStringList)_termLists[_facetIndex[i]]).get(_orderArrays[_facetIndex[i]].get(_innerScorer.docID()));
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

}
