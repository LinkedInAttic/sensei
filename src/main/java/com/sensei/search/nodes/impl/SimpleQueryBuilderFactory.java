package com.sensei.search.nodes.impl;

import org.apache.lucene.queryParser.QueryParser;

import com.sensei.search.nodes.SenseiQueryBuilder;
import com.sensei.search.nodes.SenseiQueryBuilderFactory;
import com.sensei.search.req.SenseiQuery;

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
