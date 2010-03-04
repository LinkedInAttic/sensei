package com.sensei.search.nodes.impl;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.log4j.Logger;

import proj.zoie.impl.indexing.ZoieSystem;

import com.sensei.search.nodes.SenseiIndexLoader;

public class NoopIndexLoader implements SenseiIndexLoader
{
  private static final Logger logger = Logger.getLogger(NoopIndexLoader.class);
  
  protected ZoieSystem<?,?> _zoie;
  
  public NoopIndexLoader(ZoieSystem<?,?> zoie)
  {
    _zoie = zoie;
  }
  
  public void start()
  {
    logger.info("starting NoopIndexLoader...");
  }

  public void shutdown()
  {
    logger.info("shutting down NoopIndexLoader...");
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
