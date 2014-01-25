package com.senseidb.search.query.filters;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;

import com.kamikaze.docidset.impl.NotDocIdSet;

/**
 * A NOT filter implementation.
 *
 * Since the sensei filters return upper bounds on cardinality, there is no way to estimate the cardinality of
 * a NOT in general. We would need a lower bound on cardinality to do that. Hence we go with maxDoc
 */
public class SenseiNotFilter extends SenseiFilter {
  private static final Logger log = Logger.getLogger(SenseiNotFilter.class);
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

    String plan = EMPTY_STRING;
    if(log.isDebugEnabled()) {
      plan = "NOT " + senseiDocIdSet.getQueryPlan();
    }
    return new SenseiDocIdSet(new NotDocIdSet(senseiDocIdSet.getDocIdSet(), maxDoc), docIdSetCardinality, plan);
  }
}
