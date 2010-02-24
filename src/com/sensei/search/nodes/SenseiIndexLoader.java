package com.sensei.search.nodes;

import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public interface SenseiIndexLoader
{
  public void start();
  
  public void shutdown();
  
  public long getIndexVersion();
  
  public void importIndex(ReadableByteChannel channel);
  
  public void exportIndex(WritableByteChannel channel);
}
