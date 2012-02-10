package com.senseidb.search.query;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Weight;

public abstract class AbstractScoreAdjuster extends Query
{
  private static final long serialVersionUID = 1L;
 
  public class ScoreAdjusterWeight extends Weight
  {
    private static final long serialVersionUID = 1L;
    
    Weight _innerWeight;

    public ScoreAdjusterWeight(Weight innerWeight) throws IOException
    {
      _innerWeight = innerWeight;
    }

    public String toString()
    {
      return "weight(" + AbstractScoreAdjuster.this + ")";
    }

    public Query getQuery()
    {
      return _innerWeight.getQuery();
    }

    public float getValue()
    {
      return _innerWeight.getValue();
    }

    public float sumOfSquaredWeights() throws IOException
    {
      return _innerWeight.sumOfSquaredWeights();
    }

    public void normalize(float queryNorm)
    {
      _innerWeight.normalize(queryNorm);
    }

    @Override
    public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException
    {
      Scorer innerScorer = _innerWeight.scorer(reader, scoreDocsInOrder, topScorer);
      return createScorer(innerScorer, reader, scoreDocsInOrder, topScorer);
    }

    public Explanation explain(IndexReader reader, int doc) throws IOException
    {
      Explanation innerExplain = _innerWeight.explain(reader, doc);
      return createExplain(innerExplain, reader, doc);
    }

  }

  protected final Query _query;
  
  public AbstractScoreAdjuster(Query query)
  {
    _query = query;
  }
  
  protected abstract Scorer createScorer(Scorer innerScorer, IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException;
  
  protected Explanation createExplain(Explanation innerExplain,
                                                  IndexReader reader,
                                                  int doc)
  {
    return innerExplain;
  }
  
  @Override
  public Weight createWeight(Searcher searcher) throws IOException
  {
    return new ScoreAdjusterWeight(_query.createWeight(searcher));
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException
  {
    _query.rewrite(reader);
    return this;
  }
  
  @Override
  public String toString(String field)
  {
    return _query.toString(field);
  }
  
  
  @Override
  public void extractTerms(Set terms) {
    _query.extractTerms(terms);
  }
}
