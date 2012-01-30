package com.senseidb.search.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

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
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.util.BigSegmentedArray;

public class RelevanceQuery extends AbstractScoreAdjuster
{

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
//                   {"var_facet_int":{"f":"year"}},
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
          hm_type.put(symbol, "SET");
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
            hm_type.put(symbol, "INT");
          }
          else if ("var_double".equals(type))
          {
            hm_var.put(symbol, sets.getDouble(symbol));
            hm_type.put(symbol, "DOUBLE");
          }
          else if ("var_float".equals(type))
          {
            hm_var.put(symbol, ((float)sets.getDouble(symbol)));
            hm_type.put(symbol, "FLOAT");
          }
          else if ("var_long".equals(type))
          {
            hm_var.put(symbol, sets.getLong(symbol));
            hm_type.put(symbol, "LONG");
          }
          else if ("var_string".equals(type))
          {
            hm_var.put(symbol, sets.getString(symbol));
            hm_type.put(symbol, "STRING");
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
          hm_type.put(symbol, "BOOLEAN");
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
            hm_type.put(symbol, "LONG");
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
            hm_type.put(symbol, "INNER_SCORE");
          }
        }
      }
      
      // var_facet_int, var_facet_string, var_facet_double, var_facet_long
      // {"var_facet_int":{"f":"year"}},
      else if("var_facet_int".equals(type)) 
      {
        JSONObject set = stat.optJSONObject(type);
        handleFacetSymbols(set, "FACET_INT");
      }
      else if("var_facet_double".equals(type))
      {
        JSONObject set = stat.optJSONObject(type);
        handleFacetSymbols(set, "FACET_DOUBLE");
      }
      else if("var_facet_float".equals(type))
      {
        JSONObject set = stat.optJSONObject(type);
        handleFacetSymbols(set, "FACET_FLOAT");
      }
      else if("var_facet_long".equals(type))
      {
        JSONObject set = stat.optJSONObject(type);
        handleFacetSymbols(set, "FACET_LONG");
      }
      else if("var_facet_string".equals(type))
      {
        JSONObject set = stat.optJSONObject(type);
        handleFacetSymbols(set, "FACET_STRING");
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
      if(type.startsWith("FACET"))
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
    sb.append("public float score(Object[] objs) {");
    for(int i=0; i< lls_params.size();i++)
    {
      String paramName = lls_params.get(i);
      
      if(!hm_type.containsKey(paramName))
        throw new JSONException("function arameter " + paramName + " is not defined.");
      
      if(hm_type.get(paramName).equals("INT") || hm_type.get(paramName).equals("FACET_INT"))
      {
        sb.append(" int " + paramName + " = ((Integer) objs["+ i + "]).intValue(); ");
      }
      else if(hm_type.get(paramName).equals("LONG") || hm_type.get(paramName).equals("FACET_LONG"))
      {
        sb.append(" long " + paramName + " = ((Long) objs["+ i + "]).longValue();  ");
      }
      else if(hm_type.get(paramName).equals("DOUBLE") || hm_type.get(paramName).equals("FACET_DOUBLE"))
      {
        sb.append(" double " + paramName + " = ((Double) objs["+ i + "]).doubleValue(); ");
      }
      else if(hm_type.get(paramName).equals("FLOAT") || hm_type.get(paramName).equals("FACET_FLOAT"))
      {
        sb.append(" float " + paramName + " = ((Float) objs["+ i + "]).floatValue(); ");
      }      
      else if(hm_type.get(paramName).equals("STRING") || hm_type.get(paramName).equals("FACET_STRING"))
      {
        sb.append(" String " + paramName + " = (String) objs["+ i + "]; ");
      }
      else if(hm_type.get(paramName).equals("BOOLEAN") || hm_type.get(paramName).equals("FACET_BOOLEAN"))
      {
        sb.append(" boolean " + paramName + " = ((Boolean) objs["+ i + "]).booleanValue(); ");
      }
      else if(hm_type.get(paramName).equals("SET"))
      {
        sb.append(" Set " + paramName + " = (Set) objs["+ i + "]; ");
      }
      else if(hm_type.get(paramName).equals("INNER_SCORE"))
      {
        sb.append(" float " + paramName + " = ((Float) objs["+ i + "]).floatValue(); ");
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
      
      for(int i=0; i< paramSize; i++)
      {
        if(hm_type.get(lls_params.get(i)).equals("INNER_SCORE")){
          types[i] = 0;  //inner_score type parameter;
          facetIndex[i] = -1;  //should not be used;
        }
        else if (hm_type.get(lls_params.get(i)).startsWith("FACET"))
        {
          types[i] = 1;  //facet type parameter;
          String facetName = hm_symbol_facet.get(lls_params.get(i));
          int index = hm_facet_index.get(facetName);
          facetIndex[i] = index;  // record the facet index;
        }
        else
        {
          types[i] = 2;  //normal type parameter;
          facetIndex[i] = -1;  // should not be used;
        }
      }
      

      return new CodeGenScorer(innerScorer, cscorer, orderArrays, termLists, types, facetIndex, paramSize);
//      return new Scorer(innerScorer.getSimilarity()){
//    
//        @Override
//        public float score() throws IOException {
//         
//          //prepare parameters; //for this parameter passing method, it will cost 9ms for 1000000 doc scan;
//          for(int i=0; i< paramSize; i++)
//          {
//            if(types[i]==0){
////              logger.info("==innerscore i is:"+i);
//              objs[i] = innerScorer.score();
//            }
//            else if (types[i]==1)
//            {
////              logger.info("==facet i is:"+i);
//              int index = facetIndex[i];
//              objs[i] = termLists[index].getRawValue(orderArrays[index].get(innerScorer.docID()));
//            }
//            else if (types[i] == 2)
//            {
////              logger.info("==else i is:"+i);
//              objs[i] = hm_var.get(lls_params.get(i));
//            }
//          }
//          
//          return cscorer.score(objs);
//        }
//
//        @Override
//        public int advance(int target) throws IOException {
//          return innerScorer.advance(target);
//        }
//
//        @Override
//        public int docID() {
//          return innerScorer.docID();
//        }
//
//        @Override
//        public int nextDoc() throws IOException {
//          return innerScorer.nextDoc();
//        }
//        
//      };
    }
    else{
      return innerScorer;
    }
  }

  public class  CodeGenScorer extends Scorer{

    final Scorer _innerScorer;
    final CustomScorer _cscorer;
    
    final BigSegmentedArray[] _orderArrays;
    final TermValueList[] _termLists;
    
    final int[] _types;
    final int[] _facetIndex;
    
    final int _paramSize;
    
    final Object[] _objs;
    
    public CodeGenScorer(Scorer innerScorer, 
                         CustomScorer cscorer, 
                         BigSegmentedArray[] orderArrays,
                         TermValueList[] termLists,
                         int[] types,
                         int[] facetIndex,
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
      _paramSize = paramSize;
      
      _objs = new Object[_paramSize];
      
    }
    @Override
    public float score() throws IOException {
      
      //prepare parameters; //for this parameter passing method, it will cost 9ms for 1000000 doc scan;
      for(int i=0; i< _paramSize; i++)
      {
        if(_types[i]==0){
//          logger.info("==innerscore i is:"+i);
          _objs[i] = _innerScorer.score();
        }
        else if (_types[i]==1)
        {
//          logger.info("==facet i is:"+i);
          int index = _facetIndex[i];
          _objs[i] = _termLists[index].getRawValue(_orderArrays[index].get(_innerScorer.docID()));
        }
        else if (_types[i] == 2)
        {
//          logger.info("==else i is:"+i);
          _objs[i] = hm_var.get(lls_params.get(i));
        }
      }
      
      return _cscorer.score(_objs);
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
      
      
      // calculate the score below;
      
          Object[] objs = new Object[paramSize];  //for this parameter passing method, it will cost 9ms for 1000000 doc scan;
          //prepare parameters;
          for(int i=0; i< paramSize; i++)
          {
            if(hm_type.get(lls_params.get(i)).equals("INNER_SCORE"))
              objs[i] = innerExplain.getValue();
            else if (hm_type.get(lls_params.get(i)).startsWith("FACET"))
            {
              String facetName = hm_symbol_facet.get(lls_params.get(i));
              int index = hm_facet_index.get(facetName);
              objs[i] = termLists[index].getRawValue(orderArrays[index].get(doc));
            }
            else
              objs[i] = hm_var.get(lls_params.get(i));
          }
      finalExpl.setValue(cscorer.score(objs));
      finalExpl.setDescription("Custom score: "+ cscorer.score(objs));
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
