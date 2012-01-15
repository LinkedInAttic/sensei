package com.senseidb.search.node;

import com.senseidb.search.req.SenseiQuery;

public interface SenseiQueryBuilderFactory
{
  SenseiQueryBuilder getQueryBuilder(SenseiQuery query) throws Exception;
}
