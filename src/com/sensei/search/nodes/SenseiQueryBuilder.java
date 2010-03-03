package com.sensei.search.nodes;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Query;

import com.sensei.search.req.SenseiQuery;

public interface SenseiQueryBuilder
{
  Query buildQuery(SenseiQuery req) throws ParseException;
}
