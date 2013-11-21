package com.senseidb.search.query.filters;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;

import com.kamikaze.docidset.impl.NotDocIdSet;

/**
 * A NOT filter implementation.
 *
 * Since the sensei filters return upper bounds on cardinality, there is no way to estimate the cardinality of
 * a NOT in general. We would need a lower bound on cardinality to do that. Hence we go with maxDoc
 */
public class SenseiNotFilter extends SenseiFilter {
  private static final long serialVersionUID = 1L;

  private final SenseiFilter _innerFilter;

  public SenseiNotFilter(SenseiFilter innerFilter)
  {
    _innerFilter = innerFilter;
  }

  @Override
  public SenseiDocIdSet getSenseiDocIdSet(IndexReader reader) throws IOException {
    SenseiDocIdSet senseiDocIdSet = _innerFilter.getSenseiDocIdSet(reader);
    int maxDoc = reader.maxDoc();
    DocIdSetCardinality docIdSetCardinality = senseiDocIdSet.getCardinalityEstimate().clone();
    docIdSetCardinality.invert();
    return new SenseiDocIdSet(new NotDocIdSet(senseiDocIdSet.getDocIdSet(), maxDoc), docIdSetCardinality, "NOT " + senseiDocIdSet.getQueryPlan());
  }
}
