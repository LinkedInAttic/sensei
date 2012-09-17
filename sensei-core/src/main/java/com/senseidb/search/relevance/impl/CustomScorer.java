/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
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
