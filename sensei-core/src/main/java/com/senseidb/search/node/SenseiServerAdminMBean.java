package com.senseidb.search.node;

public interface SenseiServerAdminMBean
{
  public int getId();
  public int getPort();
  public String getPartitions();
  boolean isAvailable();
  void setAvailable(boolean available);
}
