package com.senseidb.indexing.activity.time;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.senseidb.indexing.activity.ActivityPersistenceFactory.AggregatesMetadata;
import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues.IntValueHolder;
import com.senseidb.metrics.MetricsConstants;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

/**
 * Operates on top of TimeAggregatedActivityValues. 
 * It is executed every 30 seceonds. 
 * Updates the activity values for the time aggregates based on the current time. For example is the activity 5m count is 10, 
 * and one of the activity updates came into the system more than 5 mins ago, 
 * it will substract the stale activity value from ten and assign the result to the  5m time aggregated count
 * @author vzhabiuk
 *
 */
public class AggregatesUpdateJob implements Runnable {
  private final static Logger logger = Logger.getLogger(AggregatesUpdateJob.class);
  protected ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
  private final TimeAggregatedActivityValues timeAggregatedActivityValues;
  private final AggregatesMetadata aggregatesMetadata;
  private int currentCount;
  private static Timer timer = Metrics.newTimer(new MetricName(MetricsConstants.Domain,"timer","updateJob-time","agregatesUpdateJob"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  
  public AggregatesUpdateJob(TimeAggregatedActivityValues timeAggregatedActivityValues, AggregatesMetadata aggregatesMetadata) {
    this.timeAggregatedActivityValues = timeAggregatedActivityValues;
    this.aggregatesMetadata = aggregatesMetadata;
  }
  public void start() {
    executorService.scheduleAtFixedRate(this, 30, 30, TimeUnit.SECONDS);    
  }
  public void stop() {
    executorService.shutdown();    
  }
  public void awaitTermination() {    
    try {
      executorService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
  @Override
  public synchronized void run() {
    try {
      timer.time(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          runUpdateJob();
          return null;
        }
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
  }
  public void runUpdateJob() {
    int currentTime = Clock.getCurrentTimeInMinutes();
    if (currentTime <= aggregatesMetadata.getLastUpdatedTime()) {
      return;
    }
    currentCount = 0;;
    for (int i = 0; i <= timeAggregatedActivityValues.maxIndex; i++) {
      synchronized (timeAggregatedActivityValues.timeActivities.getLock(i)) {
        if (!timeAggregatedActivityValues.timeActivities.isSet(i)) {
          continue;
        }
        IntContainer activities = timeAggregatedActivityValues.timeActivities.getActivities(i);
        IntContainer times = timeAggregatedActivityValues.timeActivities.getTimes(i);
        int[] updateTempValues = new int[timeAggregatedActivityValues.intActivityValues.length];
        updateActivityValues(timeAggregatedActivityValues.intActivityValues, activities, times, currentTime, i, updateTempValues);        
      }      
    }    
    aggregatesMetadata.updateTime(currentTime);
    logger.info("Finished the AggregatesUpdateJob. Updated " + currentCount + " records");
  }
 
  private final void updateActivityValues(IntValueHolder[] intActivityValues, IntContainer activities, IntContainer times, int currentTime, int index, int[] updateTempValues) {       
    int minimumAggregateIndex = 0;
    for (int activityIndex = 0; activityIndex < activities.getSize(); activityIndex++) { 
      //the activity is current. As they are sorted in the ascending order, we can stop now
      if (times.size() != activities.size()) {
        throw new IllegalStateException("activities.size = " + activities.getSize() + ", times.size() = " + times.size());
      }
      if (times.size() == 0) {
        continue;
      }
      if (currentTime - times.get(activityIndex) < intActivityValues[intActivityValues.length - 1].timeInMinutes) {
        break;
      }
      for (int aggregateIndex = intActivityValues.length - 1; aggregateIndex >= minimumAggregateIndex; aggregateIndex--) {
        IntValueHolder intValueHolder = intActivityValues[aggregateIndex];        
        int currentElapsedTime = currentTime - times.get(activityIndex);
        //activity is current 
        if (currentElapsedTime < intValueHolder.timeInMinutes) {
          minimumAggregateIndex = aggregateIndex + 1;
          break;
        }
        int previousElapsedTime = aggregatesMetadata.getLastUpdatedTime() - times.get(activityIndex);
        //activity is not current against the current time, but was current for the previous run
        if (currentElapsedTime >= intValueHolder.timeInMinutes && previousElapsedTime < intValueHolder.timeInMinutes) {
          int activityValue = activities.get(activityIndex); 
          if (activityValue != 0) {
            updateTempValues[aggregateIndex] += activityValue;
            currentCount++;
          }
        }
      }
    }
    for (int i = 0; i < updateTempValues.length; i++) {
      int updateValue = updateTempValues[i];
      if (updateValue != 0) {
        synchronized (intActivityValues[i].activityIntValues.getFieldValues()) {
         intActivityValues[i].activityIntValues.update(index, updateValue > 0 ? String.valueOf(-updateValue) : "+" + String.valueOf(updateValue));
       }
       updateTempValues[i] = 0;
      }
    }
    //remove outdated activities
    while (true) {
      if (times.size() == 0) {
        break;
      }
      int time = times.peekFirst();
      int elapsedTime = currentTime - time;
      if (elapsedTime >= intActivityValues[0].timeInMinutes) {
        times.removeFirst();
        activities.removeFirst();
        if (times.size() == 0) {
          timeAggregatedActivityValues.timeActivities.reset(index);
        }
      } else {
        break;
      }
    }
  }
}
