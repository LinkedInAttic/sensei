package com.sensei.search.req;

import java.io.Serializable;
import java.util.Set;

// Use CRTP so subclasses can polymorphically modify the request and get the type of this back
public interface AbstractSenseiRequest<REQUEST extends AbstractSenseiRequest> extends Serializable
{
  public abstract Set<Integer> getPartitions();
  public REQUEST setPartitions(Set<Integer> partitions);
}
