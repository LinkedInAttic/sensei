package com.sensei.search.nodes;

import com.sensei.search.req.SenseiQuery;

public interface SenseiQueryBuilderFactory
{
  SenseiQueryBuilder getQueryBuilder(SenseiQuery query) throws Exception;
}
