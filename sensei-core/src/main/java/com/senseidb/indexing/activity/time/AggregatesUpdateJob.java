package com.senseidb.indexing.activity.time;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues.AggregatesMetadata;
import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues.IntValueHolder;

public class AggregatesUpdateJob implements Runnable {


  private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
  private final TimeAggregatedActivityValues timeAggregatedActivityValues;
  private final AggregatesMetadata aggregatesMetadata;

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
  @Override
  public void run() {
    int currentTime = Clock.getCurrentTimeInMinutes();
    if (currentTime <= aggregatesMetadata.lastUpdatedTime) {
      return;
    }
    for (int i = 0; i <= timeAggregatedActivityValues.maxIndex; i++) {
      synchronized (timeAggregatedActivityValues.timeActivities.getLock(i)) {
        if (!timeAggregatedActivityValues.timeActivities.isSet(i)) {
          continue;
        }
        IntContainer activities = timeAggregatedActivityValues.timeActivities.getActivities(i);
        IntContainer times = timeAggregatedActivityValues.timeActivities.getTimes(i);
        updateActivityValues(timeAggregatedActivityValues.intActivityValues, activities, times, currentTime, i);
      }      
    }    
    aggregatesMetadata.updateTime(currentTime);
  }
  private final void updateActivityValues(IntValueHolder[] intActivityValues, IntContainer activities, IntContainer times, int currentTime, int index) {
    int startIndex = 0;    
    for (int i = 0; i < activities.getSize(); i++) {      
      int j;
      for (j = startIndex, startIndex = Integer.MAX_VALUE; j < intActivityValues.length; j++) {
        IntValueHolder intValueHolder = intActivityValues[j];        
        if (currentTime - times.get(i) < intValueHolder.timeInMinutes) {
          if (startIndex > j) {
            startIndex = j;
          }
          break;
        }
        if (currentTime - times.get(i) >= intValueHolder.timeInMinutes && aggregatesMetadata.lastUpdatedTime - times.get(i) < intValueHolder.timeInMinutes) {
          intValueHolder.activityIntValues.fieldValues[index] -= activities.get(i);
          if (startIndex > j) {
            startIndex = j;
          }
        }
      }
    }
    //remove outdated activities
    while (true) {
      int time = times.peekFirst();
      if (currentTime - time >= intActivityValues[0].timeInMinutes) {
        times.removeFirst();
        activities.removeFirst();
      } else {
        break;
      }
    }
  }
  
}
