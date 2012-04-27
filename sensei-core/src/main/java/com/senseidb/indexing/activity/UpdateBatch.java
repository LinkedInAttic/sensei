package com.senseidb.indexing.activity;

import java.util.ArrayList;
import java.util.List;

import com.senseidb.indexing.activity.CompositeActivityStorage.Update;

/**
 * Keeps track of all the changes not yet persisted
 * @author vzhabiuk
 *
 * @param <T>
 */
public class UpdateBatch<T> {
  int batchSize = 50000;
  protected volatile List<T> updates = new ArrayList<T>(batchSize);
  long time = System.currentTimeMillis();
  public boolean addFieldUpdate(T fieldUpdate) {
    updates.add(fieldUpdate);
    if (flushNeeded()) {
      return true;
    }
    return false;
  }
  public boolean flushNeeded() {
    return updates.size() >= batchSize || ((System.currentTimeMillis() - time) > 15 * 1000 && !updates.isEmpty());
  }
  public List<T> getUpdates() {
    return updates;
  }
}