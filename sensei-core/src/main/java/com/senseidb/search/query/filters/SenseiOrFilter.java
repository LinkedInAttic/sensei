package com.senseidb.search.query.filters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;

import com.kamikaze.docidset.impl.OrDocIdSet;

public class SenseiOrFilter extends SenseiFilter {
  /**
   *
   */
  private static final long serialVersionUID = 1L;

  private final List<? extends SenseiFilter> _filters;

  public SenseiOrFilter(List<? extends SenseiFilter> filters)
  {
    _filters = filters;
  }

  @Override
  public SenseiDocIdSet getSenseiDocIdSet(IndexReader reader) throws IOException {
    if(_filters.size() == 1)
    {
      return _filters.get(0).getSenseiDocIdSet(reader);
    }
    else
    {
      List<DocIdSet> list = new ArrayList<DocIdSet>(_filters.size());
      int cardinalityEstimate = 0;
      for (SenseiFilter f : _filters)
      {
        SenseiDocIdSet senseiDocIdSet = f.getSenseiDocIdSet(reader);
        list.add(senseiDocIdSet.getDocIdSet());
        cardinalityEstimate += senseiDocIdSet.getCardinalityEstimate();
      }
      cardinalityEstimate = Math.min(cardinalityEstimate, reader.maxDoc());
      return new SenseiDocIdSet(new OrDocIdSet(list), cardinalityEstimate);
    }
  }
}

