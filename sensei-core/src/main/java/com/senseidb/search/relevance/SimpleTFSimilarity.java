package com.senseidb.search.relevance;

import org.apache.lucene.search.DefaultSimilarity;

/**
 *   @author Rahul Agarwal
 */
public class SimpleTFSimilarity extends DefaultSimilarity
{
  public SimpleTFSimilarity()
  {
  }

  @Override
  public float tf(float v)
  {
    return (v > 0) ? 1.0f : 0.0f;
  }
}
