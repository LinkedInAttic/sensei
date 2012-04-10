package com.senseidb.search.relevance.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.search.Scorer;
import org.json.JSONException;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.MultiValueFacetDataCache;
import com.browseengine.bobo.facets.data.TermDoubleList;
import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermShortList;
import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.util.BigSegmentedArray;
import com.senseidb.search.query.ScoreAugmentQuery.ScoreAugmentFunction;
import com.senseidb.search.relevance.CustomRelevanceFunction;
import com.senseidb.search.relevance.RuntimeRelevanceFunction;
import com.senseidb.search.relevance.impl.CompilationHelper.DataTable;

public class CustomScorer extends Scorer
{
  private static Logger logger = Logger.getLogger(CustomScorer.class);
  final Scorer _innerScorer;
  private RuntimeRelevanceFunction _sModifier = null;
  
  public CustomScorer(Scorer innerScorer, 
                      BoboIndexReader boboReader, 
                       CustomMathModel cModel, 
                       DataTable _dt,
                       JSONObject _valueJson
                       ) throws Exception
  {
    super(innerScorer.getSimilarity());
    
    _innerScorer = innerScorer;
    _sModifier = new RuntimeRelevanceFunction(cModel, _dt);
    _sModifier.initializeGlobal(_valueJson);
    _sModifier.initializeReader(boboReader, _valueJson);
  }
  

  @Override
  public float score() throws IOException {
    return _sModifier.newScore(_innerScorer.score(), docID());
  }
  
  @Override
  public int advance(int target) throws IOException {
    return _innerScorer.advance(target);
  }

  @Override
  public int docID() {
    return _innerScorer.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return _innerScorer.nextDoc();
  }
  
  
}
