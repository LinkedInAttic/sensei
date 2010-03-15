package com.sensei.search.nodes.impl;

import java.io.UnsupportedEncodingException;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;

import com.sensei.search.nodes.SenseiQueryBuilder;
import com.sensei.search.req.SenseiQuery;

public class SimpleQueryBuilder implements SenseiQueryBuilder
{
  protected Query _query = null;
  protected Filter _filter = null;
  
  public SimpleQueryBuilder(SenseiQuery query, QueryParser parser) throws Exception
  {
    doBuild(query, parser);
  }
  
  protected void doBuild(SenseiQuery query, QueryParser parser) throws Exception
  {
    if (query != null)
    {
      byte[] bytes = query.toBytes();
      String qString = null;
      
      try {
        qString = new String(bytes,"UTF-8");
      }
      catch (UnsupportedEncodingException e) {
        throw new ParseException(e.getMessage());
      }
      
      if (qString.length()>0){
        _query = parser.parse(qString);
      }
    }
  }
  
  public Query buildQuery() throws ParseException
  {
    return _query;
  }
  
  public Filter buildFilter()
  {
    return _filter;
  }
}
