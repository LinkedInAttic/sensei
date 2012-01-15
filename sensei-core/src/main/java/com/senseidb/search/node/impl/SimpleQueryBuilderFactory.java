package com.senseidb.search.node.impl;

import org.apache.lucene.queryParser.QueryParser;

import com.senseidb.search.node.SenseiQueryBuilder;
import com.senseidb.search.node.SenseiQueryBuilderFactory;
import com.senseidb.search.req.SenseiQuery;

public class SimpleQueryBuilderFactory implements SenseiQueryBuilderFactory
{
  private final QueryParser _parser;
  
  public SimpleQueryBuilderFactory(QueryParser parser)
  {
    _parser = parser;
  }
  
  public SenseiQueryBuilder getQueryBuilder(SenseiQuery query) throws Exception
  {
    return new SimpleQueryBuilder(query, _parser);
  }

}
