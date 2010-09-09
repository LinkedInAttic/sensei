package com.sensei.search.req;

import java.io.Serializable;

public class SenseiGenericRequest implements Serializable
{
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private String classname;
  private Serializable request;
  public String getClassname()
  {
    return classname;
  }
  public void setClassname(String classname)
  {
    this.classname = classname;
  }
  public Serializable getRequest()
  {
    return request;
  }
  public void setRequest(Serializable request)
  {
    this.request = request;
  }
}
