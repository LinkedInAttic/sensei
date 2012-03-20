package com.senseidb.search.query.filters;

import org.apache.lucene.search.Filter;
import org.json.JSONObject;

public class CustomFilterConstructor extends FilterConstructor
{

  public static final String FILTER_TYPE = "custom";
  
  
//  // custom
//  "custom" : {
//    "class" : "com.sensidb.query.TestFilter"
//  }
  
  @Override
  protected Filter doConstructFilter(Object json) throws Exception
  {
    try
    {
      String className = ((JSONObject)json).getString(CLASS_PARAM);
      Class filterClass = Class.forName(className);

      Filter f = (Filter)filterClass.newInstance();
      return f;
    }
    catch(Throwable t)
    {
      throw new IllegalArgumentException(t.getMessage());
    }
  }

}
