package com.senseidb.ba.plugins;

import com.senseidb.search.node.SenseiQueryBuilder;
import com.senseidb.search.node.SenseiQueryBuilderFactory;
import com.senseidb.search.req.SenseiQuery;

public class ZeusQueryBuilderFactory implements SenseiQueryBuilderFactory {
  ZeusQueryBuilder queryBuilder = new ZeusQueryBuilder();
  @Override
  public SenseiQueryBuilder getQueryBuilder(SenseiQuery query) throws Exception {
    return queryBuilder;
  }

}
