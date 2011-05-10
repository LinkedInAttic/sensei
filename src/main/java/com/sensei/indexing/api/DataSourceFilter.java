package com.sensei.indexing.api;

import org.json.JSONObject;

public interface DataSourceFilter<D>
{
  JSONObject filter(D data) throws Exception;
}

