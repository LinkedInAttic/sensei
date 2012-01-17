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
