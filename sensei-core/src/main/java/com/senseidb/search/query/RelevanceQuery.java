package com.senseidb.search.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.util.BigSegmentedArray;

public class RelevanceQuery extends AbstractScoreAdjuster
{

  private static final long serialVersionUID = 1L;
  
  protected final Query _query;
  
  private HashMap<String, Object> hm_var = new HashMap<String, Object>();
  private HashMap<String, String> hm_type = new HashMap<String, String>();
  private HashMap<String, String> hm_symbol_facet = new HashMap<String, String>();
  private HashMap<String, Integer> hm_facet_index = new HashMap<String, Integer>();
  private LinkedList<String> lls_params = new LinkedList<String>();
  private String funcBody = null;
  private CustomScorer cscorer = null;
  private int facetIndex = 0;
  
  
  static ClassPool pool = ClassPool.getDefault();
  static
  {
    pool.importPackage("java.util");
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
      else if("var_int".equals(type) || "var_double".equals(type) || "var_long".equals(type) || "var_string".equals(type))
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
        handleFacetSymbols(set);
      }
      else if("var_facet_double".equals(type))
      {
        JSONObject set = stat.optJSONObject(type);
        handleFacetSymbols(set);
      }
      else if("var_facet_long".equals(type))
      {
        JSONObject set = stat.optJSONObject(type);
        handleFacetSymbols(set);
      }
      else if("var_facet_string".equals(type))
      {
        JSONObject set = stat.optJSONObject(type);
        handleFacetSymbols(set);
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
        funcBody = stat.optString("function");
      }
      
    } // end of all the statements;
    
    if(funcBody == null)
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
      if("FACET".equals(type))
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
    
    String className = "CRel"+funcBody.hashCode();
    if(hmModels.containsKey(className))
      cscorer = hmModels.get(className);
    else
    {
      CtClass ch = pool.makeClass(className);

      try
      {
        CtClass ci = pool.get("com.senseidb.search.query.CustomScorer");
        ch.addInterface(ci);
        String functionString = makeFuncString(funcBody, hm_type, lls_params);
        CtMethod m = CtNewMethod.make(functionString, ch);
        ch.addMethod(m);
        Class h = ch.toClass();
        cscorer = (CustomScorer)h.newInstance();
        hmModels.put(className, cscorer);
      }
      catch (Exception e)
      {
        throw new JSONException(e);
      }
    }
  }



  private void handleFacetSymbols(JSONObject set) throws JSONException
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
      hm_type.put(symbol, "FACET");
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
      
      if(hm_type.get(paramName).equals("INT"))
      {
        sb.append(" int " + paramName + " = ((Integer) objs["+ i + "]).intValue(); ");
      }
      else if(hm_type.get(paramName).equals("LONG"))
      {
        sb.append(" long " + paramName + " = ((Long) objs["+ i + "]).longValue();  ");
      }
      else if(hm_type.get(paramName).equals("DOUBLE"))
      {
        sb.append(" double " + paramName + " = ((Double) objs["+ i + "]).doubleValue(); ");
      }
      else if(hm_type.get(paramName).equals("STRING"))
      {
        sb.append(" String " + paramName + " = (String) objs["+ i + "]; ");
      }
      else if(hm_type.get(paramName).equals("BOOLEAN"))
      {
        sb.append(" boolean " + paramName + " = ((Boolean) objs["+ i + "]).booleanValue(); ");
      }
      else if(hm_type.get(paramName).equals("SET"))
      {
        sb.append(" Set " + paramName + " = (Set) objs["+ i + "]; ");
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
      
      return new Scorer(innerScorer.getSimilarity()){
    
        @Override
        public float score() throws IOException {
          Object[] objs = new Object[paramSize];  //for this parameter passing method, it will cost 9ms for 1000000 doc scan;
          
         
          //prepare parameters;
          for(int i=0; i< paramSize; i++)
          {
            if(hm_type.get(lls_params.get(i)).equals("INNER_SCORE"))
              objs[i] = innerScorer.score();
            else if (hm_type.get(lls_params.get(i)).equals("FACET"))
            {
              String facetName = hm_symbol_facet.get(lls_params.get(i));
              int index = hm_facet_index.get(facetName);
              objs[i] = termLists[index].getRawValue(orderArrays[index].get(innerScorer.docID()));
            }
            else
              objs[i] = hm_var.get(lls_params.get(i));
          }
          return cscorer.score(objs);
        }

        @Override
        public int advance(int target) throws IOException {
          return innerScorer.advance(target);
        }

        @Override
        public int docID() {
          return innerScorer.docID();
        }

        @Override
        public int nextDoc() throws IOException {
          return innerScorer.nextDoc();
        }
        
      };
    }
    else{
      return innerScorer;
    }
  }

}
