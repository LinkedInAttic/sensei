package com.sensei.search.nodes.impl;

import java.io.UnsupportedEncodingException;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;

import com.sensei.search.nodes.SenseiQueryBuilder;
import com.sensei.search.req.SenseiQuery;

public class SimpleQueryBuilder implements SenseiQueryBuilder
{
  private final QueryParser _parser;
  
  public SimpleQueryBuilder(QueryParser parser)
  {
    _parser = parser;
  }
  
  public Query buildQuery(SenseiQuery query) throws ParseException
  {
	if (query == null) return null;
	byte[] bytes = query.toBytes();
	String qString = null;
	
	try {
		qString = new String(bytes,"UTF-8");
	} catch (UnsupportedEncodingException e) {
		throw new ParseException(e.getMessage());
	}
	
    if (qString.length()>0){
    	return _parser.parse(qString);
    }
    else{
    	return null;
    }
  }
}
