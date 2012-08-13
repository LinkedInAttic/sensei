package com.senseidb.indexing.activity;

import java.util.List;

public class ActivityInMemoryFactory extends ActivityPersistenceFactory {
  @Override
  protected CompositeActivityStorage getCompositeStorage(String indexDirPath) {
    return new CompositeInMemoryStorage(indexDirPath);
  }

  @Override
  protected ActivityIntStorage getActivivityIntStorage(String indexDirPath, String fieldName) {
    return new InMemoryIntStorage(fieldName, indexDirPath);
  }

  @Override
  public Metadata createMetadata(String indexDir) {
    return new InMemoryMetadata(indexDir);
  }

  @Override
  public AggregatesMetadata createAggregatesMetadata(String dirPath, String fieldName) {
    return new InMemoryAggregatesMetadata();
  }

  private static class CompositeInMemoryStorage extends CompositeActivityStorage {
    public CompositeInMemoryStorage(String indexDir) {
      super(indexDir);
    }

    @Override
    public synchronized void close() {
    }

    @Override
    public synchronized void init() {
    }

    @Override
    public synchronized void flush(List<Update> updates) {
    }

    @Override
    protected CompositeActivityValues getActivityDataFromFile(Metadata metadata) {
      CompositeActivityValues ret = new CompositeActivityValues();
      ret.activityStorage = this;
      ret.init();
      return ret;
    }
  }

  private static class InMemoryIntStorage extends ActivityIntStorage {
    private volatile boolean closed = false;

    public InMemoryIntStorage(String fieldName, String indexDir) {
      super(fieldName, indexDir);

    }

    @Override
    public synchronized void close() {
      closed = true;
    }

    @Override
    public synchronized void flush(List<FieldUpdate> updates) {
    }

    @Override
    public synchronized void init() {
    }

    @Override
    public boolean isClosed() {
      return closed;
    }

    @Override
    protected ActivityIntValues getActivityDataFromFile(int count) {
      ActivityIntValues ret = new ActivityIntValues();
      ret.activityFieldStore = this;
      ret.fieldName = fieldName;
      ret.init();
      return ret;
    }
  }

  private static class InMemoryMetadata extends Metadata {
   
    public InMemoryMetadata(String indexDir) {
      super(indexDir);
    }

    public void init() {
    }

    public void update(String version, int count) {
      this.version = version;
      this.count = count;
    }

    protected void init(String str) {
    }
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
