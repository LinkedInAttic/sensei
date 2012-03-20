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
    int minimumAggregateIndex = 0;
    for (int activityIndex = 0; activityIndex < activities.getSize(); activityIndex++) { 
      //the activity is current. As they are sorted in the ascending order, we can stop now
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
        int previousElapsedTime = aggregatesMetadata.lastUpdatedTime - times.get(activityIndex);
        //activity is not current against the current time, but was current for the previous run
        if (currentElapsedTime >= intValueHolder.timeInMinutes && previousElapsedTime < intValueHolder.timeInMinutes) {
          intValueHolder.activityIntValues.fieldValues[index] -= activities.get(activityIndex);
        }
      }
    }
    //remove outdated activities
    while (true) {
      int time = times.peekFirst();
      int elapsedTime = currentTime - time;
      if (elapsedTime >= intActivityValues[0].timeInMinutes) {
        times.removeFirst();
        activities.removeFirst();
      } else {
        break;
      }
    }
  }
}
