package com.sensei.search.nodes.impl;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;

import com.sensei.search.nodes.SenseiQueryBuilder;
import com.sensei.search.req.SenseiRequest;

public class SimpleQueryBuilder implements SenseiQueryBuilder
{
  private final QueryParser _parser;
  
  public SimpleQueryBuilder(QueryParser parser)
  {
    _parser = parser;
  }
  
  public Query buildQuery(SenseiRequest req) throws ParseException
  {
    String qString = (String)req.getQuery();
    if (qString != null && qString.length()>0){
        return _parser.parse(qString);
    }
    return null;
  }
}
