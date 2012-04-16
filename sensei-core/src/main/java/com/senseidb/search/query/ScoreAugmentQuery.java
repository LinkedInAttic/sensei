package com.senseidb.search.query;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.json.JSONException;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboIndexReader;

public class ScoreAugmentQuery extends AbstractScoreAdjuster
{

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  
  public static interface ScoreAugmentFunction{
    
    /**
     * Initialize the function object if it has to store any reader-specific data; This will be called when creating the scorer;
     * 
     * @param reader
     * @param jsonParams  
     * @throws IOException
     * 
     * This method initialize the internal score calculator, e.g., let it know what external data can be used, or anything required for computing the score;
     */
    public void initializeReader(BoboIndexReader reader, JSONObject jsonParams) throws IOException;
    
    
    /**
     * Initialize the function in Query level, which means a global initialization; Such as accessing the external storage through a network connection, or any data could be reused;
     * 
     * @param jsonParams  This JSONObject contains everything needed to initialize the scoreFunction from outside world. Just set it to NULL and ignore it in the method body if you don't want it.
     * @throws JSONException
     */
    public void initializeGlobal(JSONObject jsonParams) throws JSONException;
    
    
    /**
     * @return whether the innerscore will be used or not. If innerScore is used, newScore(float rawScore, int docID) will be called; Otherwise newScore(int docID) will be called.
     */
    public boolean useInnerScore();
    
    /**
     * @param rawScore
     * @param docID
     * @return the modified new score for document with the original innerScore;
     */
    public float newScore(float rawScore, int docID);
    
    
    /**
     * 
     * @param rawScore
     * @param docID
     * @return the modified new score for document without the original innerScore to save time;
     */
    public float newScore(int docID);
    
    /**
     * @return the String to explain how the new score is generated;
     */
    public String getExplainString(float rawscore, int docID);
    
    
    /**
     * @return a copy of itself with the initialized global data; (Not reader-specific data) If there was no global initialization, just simply return this;
     */
    public ScoreAugmentFunction getCopy();
  }
  
  private static class AugmentScorer extends Scorer{
    private static Logger logger = Logger.getLogger(AugmentScorer.class);
    private final ScoreAugmentFunction _func;
    private final Scorer _innerScorer;

    protected AugmentScorer(BoboIndexReader reader,Scorer innerScorer,ScoreAugmentFunction func, JSONObject jsonParms) throws IOException
    {
      super(innerScorer.getSimilarity());
      _innerScorer = innerScorer;
      _func = func;
      _func.initializeReader(reader, jsonParms);
    }

    @Override
    public float score()
        throws IOException
    {
        return  (_func.useInnerScore())? _func.newScore(_innerScorer.score(), _innerScorer.docID()) :  _func.newScore(_innerScorer.docID());
    }

    @Override
    public int advance(int target)
        throws IOException
    {
      return _innerScorer.advance(target);
    }

    @Override
    public int docID()
    {
      return _innerScorer.docID();
    }

    @Override
    public int nextDoc()
        throws IOException
    {
      return _innerScorer.nextDoc();
    }
    
  }
  
  private transient ScoreAugmentFunction  _func;
  private transient JSONObject            _jsonParam;

  public ScoreAugmentQuery(Query query,ScoreAugmentFunction func, JSONObject jsonParam) throws JSONException
  {
    super(query);
    _func = func;
    _func.initializeGlobal(jsonParam);
    _jsonParam = jsonParam;
    if (_func == null) throw new IllegalArgumentException("augment function cannot be null");
  }

  @Override
  protected Scorer createScorer(Scorer innerScorer,
                                IndexReader reader,
                                boolean scoreDocsInOrder,
                                boolean topScorer)
      throws IOException
  {
    if (reader instanceof BoboIndexReader){
      return new AugmentScorer((BoboIndexReader)reader,innerScorer,_func.getCopy(), _jsonParam);
    }
    else{
      throw new IllegalStateException("reader not instance of "+BoboIndexReader.class);
    }
  }
  
  @Override
  protected Explanation createExplain(Explanation innerExplain,
                                      IndexReader reader,
                                      int doc) throws IOException
  {
    if (reader instanceof BoboIndexReader ){
      Explanation finalExpl = new Explanation();
      finalExpl.addDetail(innerExplain);
      
      _func.initializeReader((BoboIndexReader)reader, _jsonParam);
      
      float innerValue = innerExplain.getValue();
      float value = 0;
      if(_func.useInnerScore())
        value = _func.newScore(innerValue, doc);
      else
        value = _func.newScore(doc);
      
      finalExpl.setValue(value);
      finalExpl.setDescription("Custom score: "+ _func.getExplainString(innerValue, doc) );
      return finalExpl;
    }
    else{
      throw new IllegalStateException("reader not instance of "+BoboIndexReader.class);
    }
  }

}
