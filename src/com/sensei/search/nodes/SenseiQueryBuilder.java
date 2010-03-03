package com.sensei.search.nodes;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Query;

import com.sensei.search.req.SenseiRequest;

public interface SenseiQueryBuilder
{
  Query buildQuery(SenseiRequest req) throws ParseException;
}
