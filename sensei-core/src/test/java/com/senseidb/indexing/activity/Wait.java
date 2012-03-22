package com.senseidb.indexing.activity;

import junit.framework.Assert;

public class Wait {
  public static void until(long timeToWait, String failureMessage, Condition condition) {
    long time = System.currentTimeMillis();
    while (System.currentTimeMillis() - time <= timeToWait) {
      if (condition.evaluate()) {
        return;
      }
      try {
        Thread.sleep(10L);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
    Assert.assertTrue(failureMessage, condition.evaluate());
  }
  
  public static interface Condition {
    public boolean evaluate();
  }
}
