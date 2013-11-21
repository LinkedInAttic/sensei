package com.senseidb.search.query.filters;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.query.MatchAllDocIdSetIterator;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.Comparator;

public class SenseiDocIdSet {
  public static final Comparator<SenseiDocIdSet> DECREASING_CARDINALITY_COMPARATOR = new Comparator<SenseiDocIdSet>() {
    @Override
    public int compare(SenseiDocIdSet a, SenseiDocIdSet b) {
      return -a.getCardinalityEstimate().compareTo(b.getCardinalityEstimate());
    }
  };
  public static final Comparator<SenseiDocIdSet> INCREASING_CARDINALITY_COMPARATOR = new Comparator<SenseiDocIdSet> (){
    @Override
    public int compare(SenseiDocIdSet a, SenseiDocIdSet b) {
      return a.getCardinalityEstimate().compareTo(b.getCardinalityEstimate());
    }
  };

  private final DocIdSet docIdSet;
  private final DocIdSetCardinality docIdSetCardinalityEstimate;
  private final String queryPlan;

  public SenseiDocIdSet(DocIdSet docIdSet, DocIdSetCardinality docIdSetCardinalityEstimate, String queryPlan) {
    this.docIdSet = docIdSet;
    this.docIdSetCardinalityEstimate = docIdSetCardinalityEstimate;
    this.queryPlan = "[" + docIdSetCardinalityEstimate + "] " + queryPlan;
  }

  public DocIdSet getDocIdSet() {
    return docIdSet;
  }

  public DocIdSetCardinality getCardinalityEstimate() {
    return docIdSetCardinalityEstimate;
  }

  public String getQueryPlan() {
    return queryPlan;
  }

  public static SenseiDocIdSet build(RandomAccessFilter randomAccessFilter, BoboIndexReader boboIndexReader, String queryPlan) throws IOException {
    DocIdSet docIdSet = randomAccessFilter.getDocIdSet(boboIndexReader);
    double facetSelectivity = randomAccessFilter.getFacetSelectivity(boboIndexReader);
    return new SenseiDocIdSet(docIdSet, DocIdSetCardinality.exact(facetSelectivity), queryPlan);
  }

  public static SenseiDocIdSet buildMatchAll(final IndexReader reader, String queryPlan) {
    DocIdSet docIdSet = new DocIdSet() {
      @Override
      public boolean isCacheable() {
        return false;
      }

      @Override
      public DocIdSetIterator iterator() throws IOException {
        return new MatchAllDocIdSetIterator(reader);
      }
    };
    return new SenseiDocIdSet(docIdSet, DocIdSetCardinality.one(), "MATCH ALL " + queryPlan);
  }

  public static SenseiDocIdSet buildMatchNone(String queryPlan) {
    return new SenseiDocIdSet(DocIdSet.EMPTY_DOCIDSET, DocIdSetCardinality.zero(), "MATCH NONE " + queryPlan);
  }
}
