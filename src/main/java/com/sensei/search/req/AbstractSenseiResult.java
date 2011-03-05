package com.sensei.search.req;

import java.io.Serializable;

public interface AbstractSenseiResult extends Serializable
{
  public abstract long getTime();
  public abstract void setTime(long searchTimeMillis);
}
