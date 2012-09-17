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
package com.senseidb.search.query;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;

import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.facets.FacetHandler;
import com.kamikaze.docidset.impl.OrDocIdSet;
import com.senseidb.indexing.DefaultSenseiInterpreter;
import com.senseidb.indexing.MetaType;

public class TimeRetentionFilter extends Filter {

  private final String _column;
  private final int _nDays;
  private final TimeUnit _dataUnit;

  static{
    DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(MetaType.Long);
  }

  public TimeRetentionFilter(String column,int nDays,TimeUnit dataUnit){
    _column = column;
    _nDays = nDays;
    _dataUnit = dataUnit;
  }

  private DocIdSet buildFilterSet(BoboIndexReader boboReader) throws IOException{
    FacetHandler facetHandler = boboReader.getFacetHandler(_column);

    if (facetHandler!=null){
      DecimalFormat formatter = new DecimalFormat(DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(MetaType.Long));
      BrowseSelection sel = new BrowseSelection(_column);
      long duration = _dataUnit.convert(_nDays, TimeUnit.DAYS);
      long now = _dataUnit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      long from = now - duration;
      sel.addValue("["+formatter.format(from)+" TO *]");
      return facetHandler.buildFilter(sel).getDocIdSet(boboReader);
    }
    throw new IllegalStateException("no facet handler defined with column: "+_column);
  }

  @Override
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    if (reader instanceof ZoieIndexReader){
      ZoieIndexReader<BoboIndexReader> zoieReader = (ZoieIndexReader<BoboIndexReader>)reader;
      List<BoboIndexReader> decorated = zoieReader.getDecoratedReaders();


      List<DocIdSet> docIdSetList = new ArrayList<DocIdSet>(decorated.size());
      for (BoboIndexReader bobo : decorated){
        docIdSetList.add(buildFilterSet(bobo));

      }
      return new OrDocIdSet(docIdSetList);
    }
    else{
      throw new IllegalStateException("reader not instance of "+ZoieIndexReader.class);
    }
  }

}
