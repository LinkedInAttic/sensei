package com.sensei.search.nodes.impl;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import proj.zoie.impl.indexing.ZoieSystem;

import com.sensei.search.nodes.SenseiIndexLoader;

public class NoopIndexLoader implements SenseiIndexLoader
{
  protected ZoieSystem<?,?> _zoie;
  
  public NoopIndexLoader(ZoieSystem<?,?> zoie)
  {
    _zoie = zoie;
  }
  
  public void start()
  {
  }

  public void shutdown()
  {
  }

  public void exportIndex(WritableByteChannel channel)
  {
  }

  public long getIndexVersion()
  {
    try
    {
      return _zoie.getCurrentDiskVersion();
    }
    catch (IOException e)
    {
      return -1;
    }
  }

  public void importIndex(ReadableByteChannel channel)
  {
  }
}
