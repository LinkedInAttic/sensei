package com.senseidb.search.query;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ToStringUtils;


public class MatchNoneDocsQuery extends Query
{


  /**
   *  This query matches nothing, is basically for Machine oriented ConstExpQuery. Or any other dummy use cases.
   *  (When a boolean expression returns false, use this dummy MatchNoneDocsQuery)
   */
  private static final long serialVersionUID = 1L;

  public MatchNoneDocsQuery() {
    this(null);
  }

  private final String normsField;

  /**
   * @param normsField Field used for normalization factor (document boost). Null if nothing.
   */
  public MatchNoneDocsQuery(String normsField) {
    this.normsField = normsField;
  }

  private class MatchNoneScorer extends Scorer {
    final TermDocs termDocs;
    final float score;
    final byte[] norms;
    private int doc = -1;
    
    MatchNoneScorer(IndexReader reader, Similarity similarity, Weight w,
        byte[] norms) throws IOException {
      super(similarity,w);
      this.termDocs = reader.termDocs(null);
      score = w.getValue();
      this.norms = norms;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return NO_MORE_DOCS;
    }
    
    @Override
    public float score() {
      return norms == null ? score : score * getSimilarity().decodeNormValue(norms[docID()]);
    }

    @Override
    public int advance(int target) throws IOException {
      return doc = termDocs.skipTo(target) ? termDocs.doc() : NO_MORE_DOCS;
    }
  }

  private class MatchNoneDocsWeight extends Weight {
    private Similarity similarity;
    private float queryWeight;
    private float queryNorm;

    public MatchNoneDocsWeight(Searcher searcher) {
      this.similarity = searcher.getSimilarity();
    }

    @Override
    public String toString() {
      return "weight(" + MatchNoneDocsQuery.this + ")";
    }

    @Override
    public Query getQuery() {
      return MatchNoneDocsQuery.this;
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
      return new MatchNoneScorer(reader, similarity, this,
          normsField != null ? reader.norms(normsField) : null);
    }

    @Override
    public Explanation explain(IndexReader reader, int doc) {
      // explain query weight
      Explanation queryExpl = new ComplexExplanation
        (true, getValue(), "MatchNoneDocsQuery, product of:");
      if (getBoost() != 1.0f) {
        queryExpl.addDetail(new Explanation(getBoost(),"boost"));
      }
      queryExpl.addDetail(new Explanation(queryNorm,"queryNorm"));

      return queryExpl;
    }
  }

  @Override
  public Weight createWeight(Searcher searcher) {
    return new MatchNoneDocsWeight(searcher);
  }

  @Override
  public void extractTerms(Set<Term> terms) {
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("*:^");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MatchNoneDocsQuery))
      return false;
    MatchNoneDocsQuery other = (MatchNoneDocsQuery) o;
    return this.getBoost() == other.getBoost();
  }

  @Override
  public int hashCode() {
    return Float.floatToIntBits(getBoost()) ^ 0x1AA71190;
  }
}
