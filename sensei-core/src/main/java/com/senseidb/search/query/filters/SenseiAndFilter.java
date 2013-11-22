package com.senseidb.search.query.filters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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

  @Override
  public SenseiDocIdSet getSenseiDocIdSet(IndexReader reader) throws IOException {
    List<SenseiDocIdSet> senseiDocIdSets = new ArrayList<SenseiDocIdSet>(_filters.size());
    DocIdSetCardinality totalDocIdSetCardinalityEstimate = DocIdSetCardinality.one();

    for (SenseiFilter f : _filters)
    {
      SenseiDocIdSet senseiDocIdSet = f.getSenseiDocIdSet(reader);
      senseiDocIdSets.add(senseiDocIdSet);
      totalDocIdSetCardinalityEstimate.andWith(senseiDocIdSet.getCardinalityEstimate());
    }

    // Lowest cardinality filters should come first in AND
    Collections.sort(senseiDocIdSets, SenseiDocIdSet.INCREASING_CARDINALITY_COMPARATOR);

    List<DocIdSet> docIdSets = new ArrayList<DocIdSet>(senseiDocIdSets.size());
    StringBuilder queryPlan = new StringBuilder("AND(");
    for(SenseiDocIdSet senseiDocIdSet : senseiDocIdSets)
    {
      if (senseiDocIdSet != senseiDocIdSets.get(0)) {
        queryPlan.append(", ");
      }

      if (!senseiDocIdSet.getCardinalityEstimate().isOne()) {
        docIdSets.add(senseiDocIdSet.getDocIdSet());
      } else {
        queryPlan.append("SKIPPED ");
      }
      queryPlan.append(senseiDocIdSet.getQueryPlan());
    }
    queryPlan.append(")");

    if (totalDocIdSetCardinalityEstimate.isOne()) {
      return SenseiDocIdSet.buildMatchAll(reader, queryPlan.toString());
    } else if (totalDocIdSetCardinalityEstimate.isZero()) {
      return SenseiDocIdSet.buildMatchNone(queryPlan.toString());
    } else if (docIdSets.size() == 1) {
      return new SenseiDocIdSet(docIdSets.get(0), totalDocIdSetCardinalityEstimate, "TRIVIAL " + queryPlan.toString());
    } else {
      return new SenseiDocIdSet(new AndDocIdSet(docIdSets), totalDocIdSetCardinalityEstimate, queryPlan.toString());
    }
  }

}
