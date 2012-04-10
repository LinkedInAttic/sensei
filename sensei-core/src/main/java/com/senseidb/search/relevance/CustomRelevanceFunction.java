package com.senseidb.search.relevance;

import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.search.query.ScoreAugmentQuery.ScoreAugmentFunction;

public abstract class CustomRelevanceFunction implements ScoreAugmentFunction
{
  
  public CustomRelevanceFunction()
  {
  }
  
  @Override
  public abstract void initializeReader(BoboIndexReader reader, JSONObject jsonValues) throws IOException;
  
  @Override
  public abstract void initializeGlobal(JSONObject jsonParams) throws JSONException;
  
  @Override
  public abstract float newScore(float rawScore, int docID);

  @Override
  public abstract String getExplainString();
  

}
