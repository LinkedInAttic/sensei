package com.senseidb.search.query.filters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;

import com.kamikaze.docidset.impl.OrDocIdSet;

public class SenseiOrFilter extends SenseiFilter {
  private static final long serialVersionUID = 1L;
  private static final Logger log = Logger.getLogger(SenseiOrFilter.class);

  private final List<? extends SenseiFilter> _filters;


  public SenseiOrFilter(List<? extends SenseiFilter> filters)
  {
    _filters = filters;
  }

  @Override
  public SenseiDocIdSet getSenseiDocIdSet(IndexReader reader) throws IOException {
    List<SenseiDocIdSet> senseiDocIdSets = new ArrayList<SenseiDocIdSet>(_filters.size());
    DocIdSetCardinality totalDocIdSetCardinalityEstimate = DocIdSetCardinality.zero();

    for (SenseiFilter f : _filters)
    {
      SenseiDocIdSet senseiDocIdSet = f.getSenseiDocIdSet(reader);
      senseiDocIdSets.add(senseiDocIdSet);
      totalDocIdSetCardinalityEstimate.orWith(senseiDocIdSet.getCardinalityEstimate());
    }

    // Highest cardinality filters should come first in OR
    Collections.sort(senseiDocIdSets, SenseiDocIdSet.DECREASING_CARDINALITY_COMPARATOR);

    List<DocIdSet> docIdSets = new ArrayList<DocIdSet>(senseiDocIdSets.size());

    StringBuilder queryPlan = log.isDebugEnabled() ? new StringBuilder() : null;
    if(log.isDebugEnabled()) {
      if(docIdSets.size() == 1) {
        queryPlan.append("TRIVIAL ");
      }
      queryPlan.append("OR(");
    }

    for(SenseiDocIdSet senseiDocIdSet : senseiDocIdSets)
    {
      if (log.isDebugEnabled() && senseiDocIdSet != senseiDocIdSets.get(0)) {
        queryPlan.append(", ");
      }
      if (!senseiDocIdSet.getCardinalityEstimate().isZero()) {
        docIdSets.add(senseiDocIdSet.getDocIdSet());
      } else if(log.isDebugEnabled()) {
        queryPlan.append("SKIPPED ");
      }

      if(log.isDebugEnabled()) {
        queryPlan.append(senseiDocIdSet.getQueryPlan());
      }
    }
    if(log.isDebugEnabled()) {
      queryPlan.append(")");
    }
    String plan = log.isDebugEnabled() ? queryPlan.toString() : EMPTY_STRING;

    if (totalDocIdSetCardinalityEstimate.isOne()) {
      return SenseiDocIdSet.buildMatchAll(reader, plan);
    } else if (totalDocIdSetCardinalityEstimate.isZero()) {
      return SenseiDocIdSet.buildMatchNone(plan);
    } else if (docIdSets.size() == 1) {
      return new SenseiDocIdSet(docIdSets.get(0), totalDocIdSetCardinalityEstimate, plan);
    } else {
      return new SenseiDocIdSet(new OrDocIdSet(docIdSets), totalDocIdSetCardinalityEstimate, plan);
    }
  }
}

