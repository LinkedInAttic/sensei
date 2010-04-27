package com.sensei.search.nodes;

public interface SenseiServerAdminMBean
{
  public int getId();
  public int getPort();
  public String getPartitions();
  boolean isAvailable();
  void setAvailable(boolean available);
}
