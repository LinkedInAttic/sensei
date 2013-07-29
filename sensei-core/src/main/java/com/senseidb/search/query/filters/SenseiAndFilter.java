package com.senseidb.search.query.filters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;

import com.kamikaze.docidset.impl.AndDocIdSet;

/**
 * An AND filter.
 * Currently uses an upper bound of the cardinality estimate by taking the maximum possible number of
 * documents returned by a sub-filter
 */
public class SenseiAndFilter extends SenseiFilter
{

  private static final long serialVersionUID = 1L;

  private final List<? extends SenseiFilter> _filters;

  public SenseiAndFilter(List<? extends SenseiFilter> filters)
  {
    _filters = filters;
  }

  /**
   * Returns lowest cardinality sets first.
   */
  private static class SenseiDocIdSetComparator implements Comparator<SenseiDocIdSet> {
    @Override
    public int compare(SenseiDocIdSet a, SenseiDocIdSet b) {
      if(a.getCardinalityEstimate() < b.getCardinalityEstimate()) {
        return -1;
      } else if(a.getCardinalityEstimate() == b.getCardinalityEstimate()) {
        return 0;
      } else {
        return 1;
      }
    }
  }

  public static final Comparator<SenseiDocIdSet> SENSEI_DOC_ID_SET_COMPARATOR = new SenseiDocIdSetComparator();

  @Override
  public SenseiDocIdSet getSenseiDocIdSet(IndexReader reader) throws IOException {
    if (_filters.size() == 1)
    {
      return _filters.get(0).getSenseiDocIdSet(reader);
    }
    else
    {
      List<SenseiDocIdSet> senseiDocIdSets = new ArrayList<SenseiDocIdSet>(_filters.size());
      int cardinalityEstimate = reader.maxDoc();

      for (SenseiFilter f : _filters)
      {
        SenseiDocIdSet senseiDocIdSet = f.getSenseiDocIdSet(reader);
        senseiDocIdSets.add(senseiDocIdSet);
        cardinalityEstimate = Math.min(cardinalityEstimate, senseiDocIdSet.getCardinalityEstimate());
      }

      // Lowest cardinality filters should come first in the AND
      Collections.sort(senseiDocIdSets, SENSEI_DOC_ID_SET_COMPARATOR);

      List<DocIdSet> docIdSets = new ArrayList<DocIdSet>(senseiDocIdSets.size());
      for(SenseiDocIdSet senseiDocIdSet : senseiDocIdSets)
      {
        docIdSets.add(senseiDocIdSet.getDocIdSet());
      }

      return new SenseiDocIdSet(new AndDocIdSet(docIdSets), cardinalityEstimate);
    }
  }
}
