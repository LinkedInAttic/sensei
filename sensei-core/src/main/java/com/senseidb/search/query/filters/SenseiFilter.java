package com.senseidb.search.query.filters;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;

import java.io.IOException;

/**
 * A filter implementation that provides an expected cardinality of the associated filter.
 * The cardinality is intended to be used to optimize filter execution order at runtime.
 * For instance, an AND filter should always begin the AND using a filter of the LOWEST cardinality to
 * reduce the number of documents considered in the result set
 */
public abstract class SenseiFilter extends Filter {
  protected static final String EMPTY_STRING = FilterConstructor.EMPTY_STRING;

  @Override
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    SenseiDocIdSet docIdSet = getSenseiDocIdSet(reader);
    return docIdSet.getDocIdSet();
  }

  public abstract SenseiDocIdSet getSenseiDocIdSet(IndexReader reader) throws IOException;

  public static SenseiFilter buildDefault(final Filter filter) {
    return buildDefault(filter, null, "UNKNOWN LUCENE FILTER");
  }

  public static SenseiFilter build(final RandomAccessFilter randomAccessFilter, final String queryPlan) throws IOException {
    return new SenseiFilter() {
      @Override
      public SenseiDocIdSet getSenseiDocIdSet(IndexReader reader) throws IOException {
        double facetSelectivity = randomAccessFilter.getFacetSelectivity((BoboIndexReader) reader);
        return new SenseiDocIdSet(randomAccessFilter.getDocIdSet(reader), DocIdSetCardinality.exact(facetSelectivity), queryPlan);
      }
    };
  }

  public static SenseiFilter buildDefault(final Filter filter, final DocIdSetCardinality suppliedDocIdSetCardinality, final String queryPlan) {
    return new SenseiFilter() {
      @Override
      public SenseiDocIdSet getSenseiDocIdSet(IndexReader reader) throws IOException {
        // TODO: There needs to be a way to estimate docIdSetCardinality of a column in general.
        // Either we can maintain a running estimate of the hit rate of a column
        // or allow a client to preload an expected estimate
        DocIdSetCardinality docIdSetCardinality = suppliedDocIdSetCardinality == null ? DocIdSetCardinality.random() : suppliedDocIdSetCardinality;
        return new SenseiDocIdSet(filter.getDocIdSet(reader), docIdSetCardinality, queryPlan);
      }
    };
  }

}
