package com.senseidb.search.relevance;

import java.io.IOException;

import org.json.JSONObject;

import com.browseengine.bobo.api.BoboIndexReader;

public class RuntimeRelevanceFunction extends CustomRelevanceFunction
{

  public RuntimeRelevanceFunction(JSONObject json)
  {
    super(json);
  }

  @Override
  public void init(BoboIndexReader reader, JSONObject json) throws IOException
  {
    // TODO Auto-generated method stub

  }

  @Override
  public float newScore(float rawScore, int docID)
  {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getExplainString()
  {
    // TODO Auto-generated method stub
    return null;
  }

}
