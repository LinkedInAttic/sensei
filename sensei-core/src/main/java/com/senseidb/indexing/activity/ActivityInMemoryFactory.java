package com.senseidb.indexing.activity;

import com.senseidb.indexing.activity.primitives.ActivityPrimitivesStorage;



public class ActivityInMemoryFactory extends ActivityPersistenceFactory {

  protected ActivityInMemoryFactory() {

    super("", new ActivityConfig());

  }
  @Override
  public AggregatesMetadata createAggregatesMetadata(String fieldName) {
    return new InMemoryAggregatesMetadata();
  }
  @Override
  public Metadata getMetadata() {
    return null;
  }
  @Override
  public ActivityPrimitivesStorage getActivivityPrimitivesStorage(String fieldName) {
    return null;
  }
  @Override
  protected CompositeActivityStorage getCompositeStorage() {
    return null;
  }
  private static class InMemoryAggregatesMetadata extends AggregatesMetadata {
    private volatile int currentTime = 0;

    public int getLastUpdatedTime() {
      return currentTime;
    }
    public void updateTime(int currentTime) {
      this.currentTime = currentTime;
    }
  }
}
