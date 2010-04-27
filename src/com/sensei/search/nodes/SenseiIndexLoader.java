package com.sensei.search.nodes;

import com.sensei.search.svc.api.SenseiException;


public interface SenseiIndexLoader
{
  public void start() throws SenseiException;
  
  public void shutdown() throws SenseiException;
}
