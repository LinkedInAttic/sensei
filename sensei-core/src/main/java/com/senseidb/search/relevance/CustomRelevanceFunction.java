package com.senseidb.search.relevance;

import java.io.IOException;

import org.json.JSONObject;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.search.query.ScoreAugmentQuery.ScoreAugmentFunction;

public abstract class CustomRelevanceFunction implements ScoreAugmentFunction
{
  private JSONObject _json;
  private boolean _initialized;
  
  public CustomRelevanceFunction(JSONObject json)
  {
    _json = json;
    _initialized = false;
  }
  
  public abstract void init(BoboIndexReader reader, JSONObject json) throws IOException;
  
  @Override
  public  void initialize(BoboIndexReader reader) throws IOException
  {
    init(reader, _json);
    _initialized = true;
  }

  @Override
  public abstract float newScore(float rawScore, int docID);

  @Override
  public abstract String getExplainString();
  
  @Override
  public boolean isInitialized()
  {
    return _initialized;
  }

}
