package com.senseidb.search.req;

import java.io.Serializable;

public interface AbstractSenseiResult extends Serializable
{
  public abstract long getTime();
  public abstract void setTime(long searchTimeMillis);
  public void addError(SenseiError error);
}
