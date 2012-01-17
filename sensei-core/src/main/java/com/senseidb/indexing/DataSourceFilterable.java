package com.senseidb.indexing;

public interface DataSourceFilterable<D>
{
  void setFilter(DataSourceFilter<D> filter);
}
