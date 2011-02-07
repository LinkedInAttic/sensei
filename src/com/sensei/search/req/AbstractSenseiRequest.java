package com.sensei.search.req;

import java.io.Serializable;
import java.util.Set;

public interface AbstractSenseiRequest extends Serializable
{
  public abstract void setPartitions(Set<Integer> partitions);
  public abstract Set<Integer> getPartitions();
}
