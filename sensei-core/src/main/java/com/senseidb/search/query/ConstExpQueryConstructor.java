package com.senseidb.search.query;

import java.util.HashSet;
import java.util.Iterator;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class ConstExpQueryConstructor extends QueryConstructor
{
  public static final String QUERY_TYPE = "const_exp";
  
  // "expression" : {
  //   "lvalue" : 4,
  //   "operator" : "in",     // supported operations: in, not_in, size, >, >=, ==, <, <=, !=,   
  //   "rvalue" : [4,5,6]
  // },
  //
  //   "in, not_in, size_is"  are set operations, set can have string, or numerical values;
  //   ">, >=, <, <=," are normal boolean operations, applied to simple numerical value; (such as an integer or double)
  //   "!=, ==" can be applied to both simple numerical value and set;
  //
  //  for set operations in or not_in, left value could be a single element, right side has to be a collection. 
  //  for set operation size_is, left size has to be a collection, and if we need to check empty set, we check if the size is 0;
  
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
    
    if(operator.equals(QueryConstructor.OP_EQUAL))
    {
      bool = checkEqual(lvalue, rvalue, json);
    }
    else if(operator.equals(QueryConstructor.OP_NOT_EQUAL))
    {
      bool = !checkEqual(lvalue, rvalue, json);
    }
    else if(operator.equals(QueryConstructor.OP_IN))
    {
      bool = checkIn(lvalue, rvalue, json);
    }
    else if(operator.equals(QueryConstructor.OP_NOT_IN))
    {
      bool = !checkIn(lvalue, rvalue, json);
    }
    else if(operator.equals(QueryConstructor.OP_SIZE_IS))
    {
      bool = checkSize(lvalue, rvalue, json);
    }
    else if(operator.equals(QueryConstructor.OP_GE))
    {
      if(lvalue instanceof JSONArray || rvalue instanceof JSONArray)
        throw new IllegalArgumentException("operator >= is not defined for list, in ExpressionQuery: " + json);
        
      double ldouble = json.getDouble(QueryConstructor.LEFT_VALUE);
      double rdouble = json.getDouble(QueryConstructor.RIGHT_VALUE);
      bool = ldouble >= rdouble;
    }
    else if(operator.equals(QueryConstructor.OP_GT))
    {
      if(lvalue instanceof JSONArray || rvalue instanceof JSONArray)
        throw new IllegalArgumentException("operator > is not defined for list, in ExpressionQuery: " + json);
        
      double ldouble = json.getDouble(QueryConstructor.LEFT_VALUE);
      double rdouble = json.getDouble(QueryConstructor.RIGHT_VALUE);
      bool = ldouble > rdouble;
    }
    else if(operator.equals(QueryConstructor.OP_LE))
    {
      if(lvalue instanceof JSONArray || rvalue instanceof JSONArray)
        throw new IllegalArgumentException("operator <= is not defined for list, in ExpressionQuery: " + json);
        
      double ldouble = json.getDouble(QueryConstructor.LEFT_VALUE);
      double rdouble = json.getDouble(QueryConstructor.RIGHT_VALUE);
      bool = ldouble <= rdouble;
    }
    else if(operator.equals(QueryConstructor.OP_LT))
    {
      if(lvalue instanceof JSONArray || rvalue instanceof JSONArray)
        throw new IllegalArgumentException("operator < is not defined for list, in ExpressionQuery: " + json);
        
      double ldouble = json.getDouble(QueryConstructor.LEFT_VALUE);
      double rdouble = json.getDouble(QueryConstructor.RIGHT_VALUE);
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

  private boolean checkSize(Object lvalue, Object rvalue, JSONObject json)
  {
    boolean bool = false;
    int size = 0;
    try{
      size = ((Integer)rvalue).intValue();
    }catch(Exception e)
    {
      throw new IllegalArgumentException("right value must be an integer for size_is operator. In ExpressionQuery: " + json);
    }
    
    if(lvalue instanceof JSONArray)
    {
      bool = ((JSONArray)lvalue).length() == size;
    }
    else
      throw new IllegalArgumentException("left value must be a list for size_is operator. In ExpressionQuery: " + json);
    
    return bool;
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
      if(lvalue.equals(rvalue))
        bool = true;
      else
        bool = false;
    }
    else
      throw new IllegalArgumentException("for == operator, left value and right value should be both simple values or both lists. in ExpressionQuery: " + json);
    
    return bool;
  }
  
}
