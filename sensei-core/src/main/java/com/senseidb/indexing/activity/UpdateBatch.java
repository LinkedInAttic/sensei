package com.senseidb.indexing.activity;

import java.util.ArrayList;
import java.util.List;

/**
 * Keeps track of all the changes not yet persisted
 * @author vzhabiuk
 *
 * @param <T>
 */
public class UpdateBatch<T> {
  int batchSize = 50000;
  protected volatile List<T> updates = new ArrayList<T>(2000);
  long delay = 15 * 1000;
  long time = System.currentTimeMillis();
  private UpdateBatch() {
  }
  public UpdateBatch(ActivityConfig activityConfig) {
    batchSize = activityConfig.getFlushBufferSize();
    delay = activityConfig.getFlushBufferMaxDelayInSeconds() * 1000;
  }
  public boolean addFieldUpdate(T fieldUpdate) {
    updates.add(fieldUpdate);
    if (flushNeeded()) {
      return true;
    }
    return false;
  }
  public boolean flushNeeded() {
    return updates.size() >= batchSize || ((System.currentTimeMillis() - time) > delay && !updates.isEmpty());
  }
  public List<T> getUpdates() {
    return updates;
  }
}