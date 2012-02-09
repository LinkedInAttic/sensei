package com.senseidb.search.req.mapred.obsolete;

import java.util.Set;

import com.senseidb.search.req.AbstractSenseiRequest;

@SuppressWarnings("rawtypes")
public class MapReduceRequest implements AbstractSenseiRequest {

  private Set<Integer> partitions;

  private MapReduceJob mapReduceJob;

  private long time;

  public MapReduceRequest(MapReduceJob mapReduceJob) {
    this.mapReduceJob = mapReduceJob;
  }

  @Override
  public void setPartitions(Set<Integer> partitions) {
    this.partitions = partitions;
  }

  @Override
  public Set<Integer> getPartitions() {

    return partitions;
  }

  @Override
  public String getRouteParam() {
    return null;
  }

  @Override
  public void saveState() {

  }

  @Override
  public void restoreState() {

  }

  public MapReduceJob getMapReduceJob() {
    return mapReduceJob;
  }
}
