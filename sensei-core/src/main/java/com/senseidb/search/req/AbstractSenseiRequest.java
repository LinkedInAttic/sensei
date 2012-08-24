package com.senseidb.search.req;

import java.io.Serializable;
import java.util.Set;

public interface AbstractSenseiRequest extends Serializable
{
  public void setPartitions(Set<Integer> partitions);
  public Set<Integer> getPartitions();
  public String getRouteParam();
//  public void saveState();
//  public void restoreState();
  public void addError(SenseiError error);
}
