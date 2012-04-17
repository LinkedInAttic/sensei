package com.senseidb.search.relevance;

import com.senseidb.search.query.ScoreAugmentQuery.ScoreAugmentFunction;

public abstract class CustomRelevanceFunction implements ScoreAugmentFunction
{
  
  public static abstract class CustomRelevanceFunctionFactory{
    
    public abstract CustomRelevanceFunction build();
  }
}
