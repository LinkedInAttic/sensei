package com.senseidb.ba.plugins;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;

import com.senseidb.search.node.SenseiQueryBuilder;

public class ZeusQueryBuilder implements SenseiQueryBuilder {

  @Override
  public Query buildQuery() throws ParseException {
    return new MatchAllDocsStaticQuery();
  }

  @Override
  public Filter buildFilter() throws ParseException {
    return null;
  }

}
