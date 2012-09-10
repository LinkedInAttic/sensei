package com.senseidb.ba.plugins;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ToStringUtils;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.ba.SegmentToZoieAdapter;

public class MatchAllDocsStaticQuery extends MatchAllDocsQuery {

  public MatchAllDocsStaticQuery() {
   
  }

  
  private class MatchAllScorer extends Scorer {
    private int doc = -1;
    private SegmentToZoieAdapter readerAdapter;
    MatchAllScorer(IndexReader reader, Similarity similarity, Weight w) throws IOException {
      super(similarity,w);
      readerAdapter = (SegmentToZoieAdapter) ((BoboIndexReader) reader).getInnerReader();
      
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return doc =  doc < readerAdapter.maxDoc() ? doc + 1 : NO_MORE_DOCS;
    }
    
    @Override
    public float score() {
      return 1f;
    }

    @Override
    public int advance(int target) throws IOException {
      doc = target - 1;
      return nextDoc();
    }
  }

  private class MatchAllDocsWeight extends Weight {
    private Similarity similarity;
    private float queryWeight;
    private float queryNorm;

    public MatchAllDocsWeight(Searcher searcher) {
      this.similarity = searcher.getSimilarity();
    }

    @Override
    public String toString() {
      return "weight(" + MatchAllDocsWeight.this + ")";
    }

    @Override
    public Query getQuery() {
      return MatchAllDocsStaticQuery.this;
    }

    @Override
    public float getValue() {
      return queryWeight;
    }

    @Override
    public float sumOfSquaredWeights() {
      queryWeight = getBoost();
      return queryWeight * queryWeight;
    }

    @Override
    public void normalize(float queryNorm) {
      this.queryNorm = queryNorm;
      queryWeight *= this.queryNorm;
    }

    @Override
    public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException {
      return new MatchAllScorer(reader, similarity, this);
    }

    @Override
    public Explanation explain(IndexReader reader, int doc) {
      // explain query weight
      Explanation queryExpl = new ComplexExplanation
        (true, getValue(), "MatchAllDocsQuery, product of:");
      if (getBoost() != 1.0f) {
        queryExpl.addDetail(new Explanation(getBoost(),"boost"));
      }
      queryExpl.addDetail(new Explanation(queryNorm,"queryNorm"));

      return queryExpl;
    }
  }

  @Override
  public Weight createWeight(Searcher searcher) {
    return new MatchAllDocsWeight(searcher);
  }

  @Override
  public void extractTerms(Set<Term> terms) {
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("*:*");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MatchAllDocsQuery))
      return false;
    MatchAllDocsQuery other = (MatchAllDocsQuery) o;
    return this.getBoost() == other.getBoost();
  }

  @Override
  public int hashCode() {
    return Float.floatToIntBits(getBoost()) ^ 0x1AA71190;
  }
}

