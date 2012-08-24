package com.senseidb.search.relevance;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.query.ScoreAugmentQuery.ScoreAugmentFunction;
import com.senseidb.search.relevance.CustomRelevanceFunction.CustomRelevanceFunctionFactory;
import com.senseidb.search.relevance.RuntimeRelevanceFunction.RuntimeRelevanceFunctionFactory;
import com.senseidb.search.relevance.impl.CompilationHelper;
import com.senseidb.search.relevance.impl.CompilationHelper.DataTable;
import com.senseidb.search.relevance.impl.CustomMathModel;
import com.senseidb.search.relevance.impl.RelevanceException;
import com.senseidb.search.relevance.impl.RelevanceJSONConstants;
import com.senseidb.search.req.ErrorType;

public class RelevanceFunctionBuilder
{

  
  
/************
 *   
"relevance":{
              //   This relevance part support both runtime anonymous model and pre-loaded relevance class;
              //      (a) The runtime model can be defined in this request, and the model will be built instantly in server and cached there, if any following request using same runtime model, it will be re-used.
              //      (b) The predefined model class has to extends "com.senseidb.search.relevance.CustomRelevanceFunction" abstract class, and constructed from Json Object;
              //
  
            // (1) Runtime model definition part; this json is used to define a runtime model (input variables, columns/facets, and function parameters and body);    
            "model":{
           
              "variables": {
                             "set_int":["c","d"],  // supported hashset types: [set_int, set_float, set_string, set_double, set_long]
                             "map_int_float":["j"],  // currently supported hashmap: [map_int_float, map_int_double, map_int_*...] [map_string_int, map_string_float, map_string_*]
                             "int":["e","f"],       // supported normal variables: [int, double, float, long, bool, string]
                             "long":["g","h"]
                            },
              "facets":{
                           "int":["year","age"],   // facet type support: [double, float, int, long, short, string];
                           "long":["time"]         // facet variable has the same name as the facet name, and they are defined inside this json;
                        },
              
               // (2) scoring function and function input parameters in Java;
               //     A scoring function and its parameters are the model. A model changes when the function body or signature changes;
               
              //  params for the function. Symbol order matters, and symbols must be those defined above. innerScore MUST be used, otherwise, makes no sense to use the custom relevance;
              //  reserved keyword for internal parameters are:  "_INNER_SCORE" and "_NOW"     

               "function_params":["_INNER_SCORE", "timeVal", "_timeWeight", "_waterworldWeight", "_half_time"],               

               // the value string in the following JSONObject is like this (a return statement MUST appear as the last one):
                     
                  //    float delta = System.currentTimeMillis() - timeVal;
                  //    float t = delta>0 ? delta : 0;
                  //    float hour = t/(1000*3600);
                  //    float timeScore = (float) Math.exp(-(hour/_half_time));
                  //    float waterworldScore = _INNER_SCORE;
                  //    float time = timeScore * _timeWeight;
                  //    float water = waterworldScore  * _waterworldWeight;
                  //    return  (time + water);
                  
               "function":" A LONG JAVA CODE STRING HERE, ONLY AS FUNCTION BODY, NEEDS RETURN STATEMENT.",
               "save_as":{
                           "name":"RuntimeModelName",
                           "overwrite":true
               }
             },
             
             //(2) Input values for the runtime model, if the model requires input values;
             "values":{
               "c":[1996,1997],
               "e":0.98,
               "j":{"key":[1,2,3], "value":[2.3, 3.4, 2.9]}      // a user input hashmap;
             },

             // (3) Pre-defined scoreFunction class;
             "predefined-model": "model-name" 
             }   
        }
        
**********/
  
  private static Logger logger = Logger.getLogger(RelevanceFunctionBuilder.class);
  
  public static ScoreAugmentFunction build(JSONObject jsonRelevance) throws JSONException
  {
    // first handle the predefined case if there is any one existing in the json;
    if(jsonRelevance.has(RelevanceJSONConstants.KW_PREDEFINED))
    {
      String modelName = jsonRelevance.getString(RelevanceJSONConstants.KW_PREDEFINED);
      
      if(ModelStorage.hasPreloadedModel(modelName))
      {
        CustomRelevanceFunctionFactory crfFactory = ModelStorage.getPreloadedModel(modelName); 
        return crfFactory.build();
      }
      else if (ModelStorage.hasRuntimeModel(modelName))
      {
        RuntimeRelevanceFunctionFactory rrfFactory = ModelStorage.getRuntimeModel(modelName);
        return rrfFactory.build();
      }
      else
      {
        throw new JSONException("No such model (CustomRelevanceFunctionFactory plugin) is registered: " + modelName);
      }
    }
    
    // runtime anonymous model;
    else if (jsonRelevance.has(RelevanceJSONConstants.KW_MODEL))
    {
      JSONObject modelJson  = jsonRelevance.optJSONObject(RelevanceJSONConstants.KW_MODEL);
      
      // build the model factory and one model; 
      // if the model factory needs to be saved, the factory will be used, otherwise, just return the model object (RuntimeRelevanceFunction);
      RuntimeRelevanceFunctionFactory rrfFactory = (RuntimeRelevanceFunctionFactory) buildModelFactoryFromModelJSON(modelJson);
      RuntimeRelevanceFunction sm = (RuntimeRelevanceFunction) rrfFactory.build();
      
      //store the model if specified;
      if(modelJson.has(RelevanceJSONConstants.KW_SAVE_AS))
      {
//        "save_as":{
//        "name":"RuntimeModelName",
//        "overwrite":true
//        }
        JSONObject jsonSaveAS = modelJson.getJSONObject(RelevanceJSONConstants.KW_SAVE_AS);
        String newRuntimeName = jsonSaveAS.getString(RelevanceJSONConstants.KW_NAME_AS);
        boolean overwrite = false;
        if(jsonSaveAS.has(RelevanceJSONConstants.KW_OVERWRITE))
          overwrite = jsonSaveAS.getBoolean(RelevanceJSONConstants.KW_OVERWRITE);
        
        if((ModelStorage.hasRuntimeModel(newRuntimeName) || ModelStorage.hasPreloadedModel(newRuntimeName)) && (overwrite == false))
          throw new IllegalArgumentException("the runtime model name " + newRuntimeName + " already exists, or you did not ask to overwrite the old model. Set \"overwrite\":true in the json will replace the old model if you want."); 

        ModelStorage.injectRuntimeModel(newRuntimeName, rrfFactory);
      }
      
      return  sm;     
    }
    else{
      throw new IllegalArgumentException("the relevance json is not valid"); 
    }
  }
  
  /**
   * A facility method to build a relevance model factory object from a relevance json object;
   * A relevance model factory can be used to generate scorefunction object as many as you want.
   * @param jsonRelevance
   * @return Relevance model Factory
   * @throws RelevanceException this could be a wrapper of JSONException, it may also provide some compilation error message.
   * Use {@link RelevanceException#getErrorCode()} method to get the error code for this exception. 
   * If the code is equal to {@link ErrorType#JsonCompilationError}, {@link RelevanceException#getMessage()} will give a basic error message,
   * while {@link RelevanceException#getCause()} will give the internal error message from JAVASSIST (more useful).
   * If the error code is equal to {@link ErrorType#JsonParsingError}, then there could be some JSON format mistake there (not compilation error).
   */
  public static CustomRelevanceFunctionFactory buildModelFactoryFromModelJSON(JSONObject modelJson) throws RelevanceException
  {
    // runtime anonymous model;
    try{
      DataTable _dt = new DataTable();
      CustomMathModel _cModel = CompilationHelper.createCustomMathScorer(modelJson, _dt);
      RuntimeRelevanceFunction sm = new RuntimeRelevanceFunction(_cModel, _dt); 
      RuntimeRelevanceFunctionFactory rrfFactory = new RuntimeRelevanceFunctionFactory(sm);
      return rrfFactory;
    }catch(JSONException e)
    {
      if(e instanceof RelevanceException)
      {
        RelevanceException re = (RelevanceException)e;
        throw (RelevanceException)e;
      }
      else
        throw new RelevanceException(ErrorType.JsonParsingError, "Json format is not correct.", e);
    }
  }
}
