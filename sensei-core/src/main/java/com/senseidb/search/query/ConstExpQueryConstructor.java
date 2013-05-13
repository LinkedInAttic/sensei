package com.senseidb.search.query;

import java.util.HashSet;
import java.util.Iterator;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;


public class ConstExpQueryConstructor extends QueryConstructor
{
  public static final String QUERY_TYPE = "const_exp";
  
  // "const_exp" : {
  //   "lvalue" : 4,
  //   "operator" : "in",     // supported operations: (1) set operations: in, not_in, ==, != ; (2) single value operations:  >, >=, ==, <, <=, !=,   
  //   "rvalue" : [4,5,6]
  // },
  //
  // "const_exp" : {
  //   "lvalue" : {
  //                 "function":"length",   // different function may have different number of parameters;
  //                 "params": [            // we use json array to represent a list of parameters;
  //                              [5,6,7]    // for function length, we only have one parameter, which is a json array;
  //                           ]
  //              },
  //   "operator" : ">",     //  this example shows how to check if a value list is not empty;   
  //   "rvalue" : 0
  // },  
  //
  //   "in, not_in"  are set operations, set can have string, or numerical values;
  //   ">, >=, <, <=," are normal boolean operations, applied to simple numerical value; (such as an integer or double)
  //   "!=, ==" can be applied to both simple numerical value and set and string;
  //
  //  for set operations in or not_in, left value could be a single element, right side has to be a collection. 
  //
  // Expression Query is mostly combined with other queries to form a boolean query, and filled by query template.
  // For example, it can be used in BQL template used by machine.

  @Override
  protected Query doConstructQuery(JSONObject json) throws JSONException
  {
    boolean bool = false;
    String operator = null;
    Object lvalue = null;
    Object rvalue = null;
    
    Iterator<String> iter = json.keys();
    if (!iter.hasNext())
      throw new IllegalArgumentException("no operator or values specified in ExpressionQuery: " + json);

    while(iter.hasNext())
    {
      String field = iter.next();
      if(field.equals(QueryConstructor.OPERATOR_PARAM))
        operator = json.optString(field);
      else if(field.equals(QueryConstructor.LEFT_VALUE))
        lvalue = json.opt(field);
      else if(field.equals(QueryConstructor.RIGHT_VALUE))
        rvalue = json.opt(field);
    }
    
    if(operator == null)
      throw new IllegalArgumentException("operator not defined in ExpressionQuery: " + json);
    if(lvalue == null)
      throw new IllegalArgumentException("left value not defined in ExpressionQuery: " + json);
    if(rvalue == null)
      throw new IllegalArgumentException("right value not defined in ExpressionQuery: " + json);
    
    // if either lvalue or rvalue has a built-in function, process this function firstly;
    // the returned result can only be json array or double value;
    
    if(lvalue instanceof JSONObject)
      lvalue = getValueFromFunction((JSONObject)lvalue);
    if(rvalue instanceof JSONObject)
      rvalue = getValueFromFunction((JSONObject)rvalue);  
      
      
    // set operations only;
    if(operator.equals(QueryConstructor.OP_IN))
    {
      bool = checkIn(lvalue, rvalue, json);
    }
    else if(operator.equals(QueryConstructor.OP_NOT_IN))
    {
      bool = !checkIn(lvalue, rvalue, json);
    }
    
    // operations for set or signal values;
    else if(operator.equals(QueryConstructor.OP_EQUAL))
    {
      bool = checkEqual(lvalue, rvalue, json);
    }
    else if(operator.equals(QueryConstructor.OP_NOT_EQUAL))
    {
      bool = !checkEqual(lvalue, rvalue, json);
    }
    
    // single value comparisons only;
    else if(operator.equals(QueryConstructor.OP_GE) || operator.equals(QueryConstructor.OP_GT) ||
            operator.equals(QueryConstructor.OP_LE) || operator.equals(QueryConstructor.OP_LT) 
            )
    {
      if(lvalue instanceof JSONArray || rvalue instanceof JSONArray)
        throw new IllegalArgumentException("operator " + operator + " is not defined for list, in ExpressionQuery: " + json);
      
      double ldouble = convertToDouble(lvalue, json);
      double rdouble = convertToDouble(rvalue, json);
      
      if(operator.equals(QueryConstructor.OP_GE))  // >=
        bool = ldouble >= rdouble;
      else if(operator.equals(QueryConstructor.OP_GT))  // >
        bool = ldouble > rdouble;
      else if(operator.equals(QueryConstructor.OP_LE))   // <=
        bool = ldouble <= rdouble;
      else if(operator.equals(QueryConstructor.OP_LT))   // <
        bool = ldouble < rdouble;
    }
    else
    {
      throw new IllegalArgumentException("Operator " + operator + " is not supported in ExpressionQuery: " + json);
    }
    
    Query q = null;
    if(bool == true)
      q = new MatchAllDocsQuery();
    else
      q = new MatchNoneDocsQuery();
    return q;
  }

  
  private double convertToDouble(Object value, JSONObject json)
  {
    if(value instanceof Number)
    {
      return ((Number) value).doubleValue();
    }
    else
      throw new IllegalArgumentException("operator >, >=, <, <= can only be applied to double, int, long or float type data, in ExpressionQuery: " + json);
  }


  /**
   * @param funcJSON
   * @return the function result can only be either a Double or a JSONArray;
   * @throws JSONException
   */
  private Object getValueFromFunction(JSONObject funcJSON) throws JSONException
  {
    String function = funcJSON.optString(QueryConstructor.FUNCTION_NAME);
    if(function.length()==0)
      throw new IllegalArgumentException("No function name is defined in ExpressionQuery's function json: " + funcJSON);
    
    JSONArray params = funcJSON.optJSONArray(QueryConstructor.PARAMS_PARAM);
    if(params == null)
      throw new IllegalArgumentException("No function param is defined in ExpressionQuery's function json: " + funcJSON);
    
    if(function.equals("length"))
    {
      // get the first and only the first parameter for length function;
      JSONArray param = params.optJSONArray(0);
      if(param == null)
        throw new IllegalArgumentException("No param is provided for function '" + function + "' defined in ExpressionQuery's function json: " + funcJSON);
      return (double)param.length();
    }
    else
      throw new IllegalArgumentException("Unsupported function '" + function + "' in ExpressionQuery's function json: " + funcJSON);
  }

  private boolean checkIn(Object lvalue, Object rvalue, JSONObject json) throws JSONException
  {
    boolean bool = false;
    if(rvalue instanceof JSONArray)
    {
      JSONArray rarray = (JSONArray) rvalue;
      HashSet hs = new HashSet();
      for(int i=0; i< rarray.length(); i++)
      {
        Object robj = rarray.get(i);
        hs.add(robj);
      }
      
      if(lvalue instanceof JSONArray)
      {
        JSONArray larray = (JSONArray) lvalue;
        bool = true;
        for(int j=0; j< larray.length(); j++)
        {
          Object lobj = larray.get(j);
          if(!hs.contains(lobj))
          {
            bool = false;
            break;
          }
        }
      }
      else if(hs.contains(lvalue))
        bool = true;
      else
        bool = false;
    }
    else
    {
      throw new IllegalArgumentException("operator not_in requires a list of objects as the right value, in ExpressionQuery: "+ json);
    }
    return bool;
  }

  private boolean checkEqual(Object lvalue, Object rvalue, JSONObject json) throws JSONException
  {
    boolean bool = false;
    if((lvalue instanceof JSONArray) && (rvalue instanceof JSONArray))
    {
      JSONArray larray = (JSONArray) lvalue;
      JSONArray rarray = (JSONArray) rvalue;
      
      if(larray.length() != rarray.length())
        bool = false;
      else
      {
        HashSet hs = new HashSet();
        for(int i=0; i< larray.length(); i++)
        {
          Object lobj = larray.get(i);
          hs.add(lobj);
        }
        
        bool = true;
        for(int j=0; j< rarray.length(); j++)
        {
          Object robj = rarray.get(j);
          if(!hs.contains(robj))
          {
            bool = false;
            break;
          }
        }
      }
    }
    else if(!(lvalue instanceof JSONArray) && !(rvalue instanceof JSONArray))
    {
      if(lvalue instanceof String && rvalue instanceof String)
        bool = lvalue.equals(rvalue);
      else
      {
        double ldouble = convertToDouble(lvalue, json);
        double rdouble = convertToDouble(rvalue, json);
        bool = ldouble == rdouble;
      }
    }
    else
      throw new IllegalArgumentException("for == operator, left value and right value should be both simple values or both lists. in ExpressionQuery: " + json);
    
    return bool;
  }
  
  
  public static void main(String args[]) throws JSONException{
    JSONObject json = new FastJSONObject();
    JSONObject func = new FastJSONObject();
    func.put("function", "length");
    func.put("params", new FastJSONArray().put(new FastJSONArray()));
    json.put("lvalue", func);
    json.put("operator", "==");
    json.put("rvalue", 0);
    
    ConstExpQueryConstructor c = new ConstExpQueryConstructor();
    c.doConstructQuery(json);
  }
  
}
