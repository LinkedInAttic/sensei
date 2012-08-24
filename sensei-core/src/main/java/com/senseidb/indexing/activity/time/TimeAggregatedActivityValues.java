package com.senseidb.indexing.activity.time;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.senseidb.indexing.activity.ActivityPersistenceFactory.AggregatesMetadata;
import com.senseidb.indexing.activity.primitives.ActivityIntValues;
import com.senseidb.indexing.activity.primitives.ActivityPrimitiveValues;
import com.senseidb.indexing.activity.ActivityPersistenceFactory;
import com.senseidb.indexing.activity.ActivityValues;

/**
 * This is the composite that correspond to such schema configuration 
 *
 * <pre>{@code  <facet name="aggregated-likes" column="likes" type="aggregated-range">
 *    <params>
 *         <param name="time" value="5m" />
 *         <param name="time" value="15m" />
 *         <param name="time" value="1h" />
 *         <param name="time" value="12h" />
 *         <param name="time" value="1d" />
 *         <param name="time" value="7d" />
 *         <param name="time" value="2w" />
 *   </params>
 *</facet>}</pre>
 *
 * getDefaultIntValues() will correspond to likes <br>
 * intActivityValues will contain all the aggregated fields, eg likes:5m, likes:15m, likes:1h etc. <br>
 * Basically this class is a composite, containing intActivityValue for each aggregate period specified in the config plus a default non time trimmed  values
 * Each of  underlying activityIntValues will be persisting themselves to the disk.<br>
 * When the TimeAggregatedActivityValues is constructed from file, it will init all the aggregated activity int values from the disk. 
 * And it will try to estimate timeHits - {@link TimeHitsHolder}
 *
 */
public class TimeAggregatedActivityValues implements ActivityValues {  
	protected final String fieldName;
	protected Map<String, ActivityIntValues> valuesMap = new HashMap<String, ActivityIntValues>();
	protected IntValueHolder[] intActivityValues;
	protected TimeHitsHolder timeActivities;
	public volatile int maxIndex;
  private AggregatesMetadata aggregatesMetadata;
  private AggregatesUpdateJob aggregatesUpdateJob;  
  protected ActivityIntValues defaultIntValues;
  
  private TimeAggregatedActivityValues(String fieldName, List<String> times, int count,  ActivityPersistenceFactory activityPersistenceFactory) {
		this.fieldName = fieldName;
		intActivityValues = new IntValueHolder[times.size()];
		int index = 0;
		for(String time : times) {
			int timeInMinutes = extractTimeInMinutes(time);			
			ActivityIntValues activityIntValues = (ActivityIntValues) ActivityPrimitiveValues.createActivityPrimitiveValues(activityPersistenceFactory, int.class, fieldName + ":" + time, count);
			  	
			this.valuesMap.put(time, activityIntValues);
			intActivityValues[index++] = new IntValueHolder(activityIntValues, time, timeInMinutes);
		}
		defaultIntValues = (ActivityIntValues) ActivityPrimitiveValues.createActivityPrimitiveValues(activityPersistenceFactory, int.class, fieldName, count);
		Arrays.sort(intActivityValues);
		maxIndex = count;
		aggregatesMetadata = activityPersistenceFactory.createAggregatesMetadata(fieldName);
		
	}
	protected synchronized static void initTimeHits(TimeHitsHolder timeActivities, IntValueHolder[] intActivityValues, int count, int lastUpdatedTime) {
    for (int index = 0; index < count; index++) {
      int activitiesCount = 0;
      for (int j = 0; j < intActivityValues.length; j++) {
        int value = intActivityValues[j].activityIntValues.getIntValue(index);
        if (value == Integer.MIN_VALUE) {
          activitiesCount = 0;
          break;
        }
        activitiesCount += value;        
      }
      if (activitiesCount == 0) {
        continue;
      }
      int length = Math.min(activitiesCount, intActivityValues[0].timeInMinutes);
      IntContainer times = new IntContainer(length);
      IntContainer activities = new IntContainer(length);
      for (int j = 0; j < intActivityValues.length - 1; j++) {
        int value = intActivityValues[j].activityIntValues.getIntValue(index);
        int time = intActivityValues[j].timeInMinutes;
        if (value == Integer.MIN_VALUE) {
          activitiesCount = 0;
          break;
        }
        activitiesCount += value;
        fillTimeHits(times, activities, value - intActivityValues[j + 1].activityIntValues.getIntValue(index), lastUpdatedTime - time + 1, time - intActivityValues[j + 1].timeInMinutes);
      }
      fillTimeHits(times, activities, intActivityValues[intActivityValues.length - 1].activityIntValues.getIntValue(index), lastUpdatedTime - intActivityValues[intActivityValues.length - 1].timeInMinutes + 1, intActivityValues[intActivityValues.length - 1].timeInMinutes);
      timeActivities.activities[index] = activities;
      timeActivities.times[index] = times;
    }
    
  }
  
  private static void fillTimeHits(IntContainer times, IntContainer activities, int activityCount, int startTime, int periodInMinutes) {
    int length = java.lang.Math.min(periodInMinutes, activityCount);
    if (length == 1) {
      activities.add(activityCount);
      times.add(startTime + periodInMinutes / 2);
    } else if (length > 1) {
      int activityIncrement = activityCount / length;
      int timeIncrement = periodInMinutes / length;
      int activityIncrementDelta = activityCount - activityIncrement * length;
      int timeOffset = startTime;
      for (int i = 0; i < length; i++) {
        if (i == 0) {
          activities.add(activityIncrementDelta + activityIncrement);
        } else {
          activities.add(activityIncrement);
        }
        times.add(timeOffset);
        timeOffset += timeIncrement;
      }
    }
  }
  public static Integer extractTimeInMinutes(String time) {
		time = time.trim();
		char identifier = time.charAt(time.length() - 1);
		int number = Integer.parseInt(time.substring(0, time.length() - 1));
		switch(identifier) {
			case 'm' : return number;
			case 'h' : return 60 * number;
			case 'd' : return 24 * 60 * number;
			case 'w' : return 7 * 24 * 60 * number;
			case 'M' : return 30 * 24 * 60 * number;
			case 'y' : return 365 * 24 * 60 * number;
			default : throw new UnsupportedOperationException("Only m, h, d, w are supported in the end of the time String");
		}
	}
	@Override
	public void init(int capacity) {
		timeActivities = new TimeHitsHolder(capacity);
		initTimeHits(timeActivities, intActivityValues, capacity, aggregatesMetadata.getLastUpdatedTime());
		aggregatesUpdateJob = new AggregatesUpdateJob(this, aggregatesMetadata);
		aggregatesUpdateJob.start();
	}

	@Override
	public boolean update(int index, Object value) {
		boolean needToFlush = false;
		if (maxIndex < index) {
		  maxIndex = index;
		}	
		int valueInt = getIntValue(value);
		String valueStr = valueInt > 0 ? "+" + valueInt : String.valueOf(valueInt);
		int currentTime = Clock.getCurrentTimeInMinutes();
		synchronized (defaultIntValues) {
		  needToFlush = needToFlush | defaultIntValues.update(index, value);
		}
		timeActivities.ensureCapacity(index);
		synchronized (timeActivities.getLock(index)) {
			if (!timeActivities.isSet(index)) {
				timeActivities.setActivities(index, new IntContainer(1));
				timeActivities.setTime(index, new IntContainer(1));
			}		
			if (timeActivities.getTimes(index).getSize() > 0 && timeActivities.getTimes(index).peekLast() == currentTime) {
			  timeActivities.getActivities(index).add(timeActivities.getActivities(index).removeLast() + valueInt);
			} else {
			  timeActivities.getTimes(index).add(currentTime);
			  timeActivities.getActivities(index).add(valueInt);
			}
		}
		for (IntValueHolder intValueHolder : intActivityValues) {			
			  synchronized (intValueHolder.activityIntValues) {
			    needToFlush = needToFlush | intValueHolder.activityIntValues.update(index, valueStr);
			  }
		}
		return needToFlush;
	}
  private int getIntValue(Object value) {
    int valueInt;
    if (value instanceof Number) {
		  valueInt = ((Number) value).intValue();
		} else if (value instanceof String) {
		  if (value.toString().startsWith("+")) {
		     valueInt = Integer.parseInt(value.toString().substring(1));
		  } else {
		    valueInt = Integer.parseInt(value.toString());
		  }
		} else {
		  throw new UnsupportedOperationException();
		}
    return valueInt;
  }

	@Override
	public void delete(int index) {	  
	  synchronized (defaultIntValues) {  
	    defaultIntValues.delete(index);
	  }
	  for (IntValueHolder intValueHolder : intActivityValues) {	 
	    synchronized (intValueHolder.activityIntValues) {
	      intValueHolder.activityIntValues.delete(index);	   
	    }
    }
	  synchronized (timeActivities.getLock(index)) {
	    timeActivities.reset(index);
	  }
	}

	@Override
	public Runnable prepareFlush() {
		final List<Runnable> flushes = new ArrayList<Runnable>(intActivityValues.length);
		flushes.add(defaultIntValues.prepareFlush());
		for (IntValueHolder intValueHolder : intActivityValues) {
			flushes.add(intValueHolder.activityIntValues.prepareFlush());
		}
		return new Runnable() {
			public void run() {
				for (Runnable runnable : flushes) {
					runnable.run();
				}
			}
		};
	}

	@Override
	public String getFieldName() {
		return fieldName;
	}

	@Override
	public void close() {
	  defaultIntValues.close();
	  aggregatesUpdateJob.stop();	  
	  for (IntValueHolder intValueHolder : intActivityValues) {
			intValueHolder.activityIntValues.close();
		}
		
	}
	
	public ActivityIntValues getDefaultIntValues() {
    return defaultIntValues;
  }

  public AggregatesUpdateJob getAggregatesUpdateJob() {
    return aggregatesUpdateJob;
  }

  /**
   * @author vzhabiuk
   *
   */
  public static class IntValueHolder implements Comparable<IntValueHolder> {
		public  ActivityIntValues activityIntValues;
		public final String time;
		public final Integer timeInMinutes;

		public IntValueHolder(ActivityIntValues activityIntValues, String time, Integer timeInMinutes) {
			this.activityIntValues = activityIntValues;
			this.time = time;
			this.timeInMinutes = timeInMinutes;
		}

		@Override
		public int compareTo(IntValueHolder obj) {
			return obj.timeInMinutes - timeInMinutes;
		}
	}
	
	/**
	 * Contains the time and values of all the relevant updates, that came to the system in the past. The value will be deleted when the 
	 * <pre>
	 * {@code
	 * longestTimeAgregate = max(valuesMap.keySet); 
	 * update.time < Clock.getCurrentTimeInMinutes - longestTimeAgregate;
	 *}</pre>
	 */
	public static class TimeHitsHolder {
		private IntContainer[] times;
		private IntContainer[] activities;
		public TimeHitsHolder(int capacity) {
			times = new IntContainer[capacity];
			activities = new IntContainer[capacity];
		}
		public IntContainer getTimes(int index) {
			return times[index];
		}
		public IntContainer getActivities(int index) {
			return activities[index];
		}
		public boolean isSet(int index) {
			return activities[index] != null;
		}
		public void reset(int index) {
		 if (activities.length <= index) {
		   return;
		 }
		 activities[index] = null;
		 times[index] = null;
		}
		public void setTime(int index, IntContainer time) {
			ensureCapacity(index);
			times[index] = time;
		}
		public void setActivities(int index, IntContainer activity) {
			ensureCapacity(index);
			activities[index] = activity;
		}
		public Object getLock(int index) {
			return activities[index] != null ? activities[index] : this;
		}
		public void ensureCapacity(int currentArraySize) {
		    if (times.length == 0) {
		    	times = new IntContainer[50000];
		    	activities = new IntContainer[50000];
		      return;
		    }
		    if (times.length - currentArraySize < 2) {
		      int newSize = times.length < 10000000 ? times.length * 2 : (int) (times.length * 1.5);
		      IntContainer[] newFieldValues = new IntContainer[newSize];
		      System.arraycopy(times, 0, newFieldValues, 0, times.length);
		      times = newFieldValues;
		      newFieldValues = new IntContainer[newSize];
		      System.arraycopy(activities, 0, newFieldValues, 0, activities.length);
		      activities = newFieldValues;
		    }
		  }
	}
	
	
	
  public Map<String, ActivityIntValues> getValuesMap() {
    return valuesMap;
  }
  public TimeHitsHolder getTimeActivities() {
    return timeActivities;
  }
  public static TimeAggregatedActivityValues createTimeAggregatedValues(String fieldName, List<String> times, int count, ActivityPersistenceFactory activityPersistenceFactory) {
    TimeAggregatedActivityValues ret = new TimeAggregatedActivityValues(fieldName, times, count, activityPersistenceFactory);
    ret.init(count > 0 ? count : 15000);
    return ret;
  }
}
