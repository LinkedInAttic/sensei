package com.sensei.search.req;

import java.io.Serializable;

public class SenseiGenericResult implements Serializable
{

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private String classname;
  private Serializable result;
  public String getClassname()
  {
    return classname;
  }
  public void setClassname(String classname)
  {
    this.classname = classname;
  }
  public Serializable getResult()
  {
    return result;
  }
  public void setResult(Serializable result)
  {
    this.result = result;
  }
}
