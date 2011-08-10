package com.sensei.indexing.api;

public interface DataSourceFilterable<D>
{
  void setFilter(DataSourceFilter<D> filter);
}
