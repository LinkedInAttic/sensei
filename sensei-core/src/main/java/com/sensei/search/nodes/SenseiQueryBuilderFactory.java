package com.sensei.search.nodes;

import com.senseidb.search.req.SenseiQuery;

public interface SenseiQueryBuilderFactory
{
  SenseiQueryBuilder getQueryBuilder(SenseiQuery query) throws Exception;
}
