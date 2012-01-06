package com.sensei.search.query;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import org.json.JSONException;
import org.json.JSONObject;


public class TextQueryConstructor extends QueryConstructor
{
  private static final Logger logger = Logger.getLogger(TextQueryConstructor.class);

  public static final String QUERY_TYPE = "text";

  // "text" : {
  //   "message" : "this is a test",   // field: "message", query: "this is a test"
  //   "operator" : "or",              // operator, possible values: "and", "or"
  //   "type" : "phrase"               // query type, can be "phrase", "phrase_prefix"
  // },

  private Analyzer _analyzer;

  public TextQueryConstructor(QueryParser qparser)
  {
    _analyzer = qparser.getAnalyzer();
  }

  @Override
  protected Query doConstructQuery(JSONObject json) throws JSONException
  {
    String op = null;
    String type = null;
    String field = null;
    String text = null;

    Iterator<String> iter = json.keys();
    if (!iter.hasNext())
      throw new IllegalArgumentException("text field not fould");

    field = iter.next();

    Object obj = json.get(field);
    if (obj instanceof JSONObject)
    {
      text = ((JSONObject)obj).getString(VALUE_PARAM);
      op   = ((JSONObject)obj).optString(OPERATOR_PARAM);
      type = ((JSONObject)obj).optString(TYPE_PARAM);
    }
    else
    {
      text = String.valueOf(obj);
    }

    TokenStream tokenStream = _analyzer.tokenStream(field, new StringReader(text));
    TermAttribute termAttribute = tokenStream.getAttribute(TermAttribute.class);

    try
    {
      if (PHRASE_PARAM.equals(type))
      {
        PhraseQuery q = new PhraseQuery();
        while (tokenStream.incrementToken())
        {
          q.add(new Term(field, termAttribute.term()));
        }
        return q;
      }
      else if (PHRASE_PREFIX_PARAM.equals(type))
      {
        MultiPhraseQuery q = new MultiPhraseQuery();
        while (tokenStream.incrementToken())
        {
          q.add(new Term(field, termAttribute.term()));
        }
        return q;
      }
      else
      {
        BooleanQuery q = new BooleanQuery();
        if (AND_PARAM.equals(op))
        {
          while (tokenStream.incrementToken())
          {
            q.add(new TermQuery(new Term(field, termAttribute.term())), BooleanClause.Occur.MUST);
          }
        }
        else
        {
          while (tokenStream.incrementToken())
          {
            q.add(new TermQuery(new Term(field, termAttribute.term())), BooleanClause.Occur.SHOULD);
          }
        }
        return q;
      }
    }
    catch(IOException ioe)
    {
      logger.error(ioe.getMessage(), ioe);
      return null;
    }
  }
}
