package com.senseidb.indexing.activity.time;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.senseidb.indexing.activity.ActivityIntValues;
import com.senseidb.indexing.activity.ActivityValues;

public class TimeAggregatedActivityValues implements ActivityValues {  
	protected final String fieldName;
	protected Map<String, ActivityIntValues> valuesMap = new HashMap<String, ActivityIntValues>();
	protected IntValueHolder[] intActivityValues;
	protected TimeHitsHolder timeActivities;
	protected volatile int maxIndex;
  private AggregatesMetadata aggregatesMetadata;
  private AggregatesUpdateJob aggregatesUpdateJob;
	
	public TimeAggregatedActivityValues(String fieldName, List<String> times, int count, String indexDirPath) {
		this.fieldName = fieldName;
		intActivityValues = new IntValueHolder[times.size()];
		int index = 0;
		for(String time : times) {
			int timeInMinutes = extractTimeInMinutes(time);			
			ActivityIntValues activityIntValues = ActivityIntValues.readFromFile(indexDirPath, fieldName + ":" + time, count);			
			this.valuesMap.put(time, activityIntValues);
			intActivityValues[index++] = new IntValueHolder(activityIntValues, time, timeInMinutes);
		}
		
		Arrays.sort(intActivityValues);
		maxIndex = count;
		aggregatesMetadata = AggregatesMetadata.init(indexDirPath, fieldName);
		
	}
	protected static void initTimeHits(TimeHitsHolder timeActivities, IntValueHolder[] intActivityValues, int count, int lastUpdatedTime) {
    for (int index = 0; index < count; index++) {
      int activitiesCount = 0;
      for (int j = 0; j < intActivityValues.length; j++) {
        int value = intActivityValues[j].activityIntValues.getValue(index);
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
        int value = intActivityValues[j].activityIntValues.getValue(index);
        int time = intActivityValues[j].timeInMinutes;
        if (value == Integer.MIN_VALUE) {
          activitiesCount = 0;
          break;
        }
        activitiesCount += value;
        fillTimeHits(times, activities, value - intActivityValues[j + 1].activityIntValues.getValue(index), lastUpdatedTime - time + 1, time - intActivityValues[j + 1].timeInMinutes);
      }
      fillTimeHits(times, activities, intActivityValues[intActivityValues.length - 1].activityIntValues.getValue(index), lastUpdatedTime - intActivityValues[intActivityValues.length - 1].timeInMinutes + 1, intActivityValues[intActivityValues.length - 1].timeInMinutes);
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
			default : throw new UnsupportedOperationException("Only m, h, d, w are supported in the end of the time String");
		}
	}
	@Override
	public void init(int capacity) {
		timeActivities = new TimeHitsHolder(capacity);
		initTimeHits(timeActivities, intActivityValues, capacity, aggregatesMetadata.lastUpdatedTime);
		aggregatesUpdateJob = new AggregatesUpdateJob(this, aggregatesMetadata);
		aggregatesUpdateJob.start();
	}

	@Override
	public boolean update(int index, Object value) {
		boolean needToFlush = false;
		if (maxIndex < index) {
		  maxIndex = index;
		}
		int valueInt = Integer.parseInt(value.toString());
		String valueStr = valueInt > 0 ? "+" + valueInt : String.valueOf(valueInt);
		int currentTime = Clock.getCurrentTimeInMinutes();
		timeActivities.ensureCapacity(index);
		synchronized (timeActivities.getLock(index)) {
			if (!timeActivities.isSet(index)) {
				timeActivities.setActivities(index, new IntContainer(1));
				timeActivities.setTime(index, new IntContainer(1));
			}
		}
		synchronized (timeActivities.getLock(index)) {
			if (timeActivities.getTimes(index).getSize() > 0 && timeActivities.getTimes(index).peekLast() == currentTime) {
			  timeActivities.getActivities(index).add(timeActivities.getActivities(index).removeLast() + valueInt);
			} else {
			  timeActivities.getTimes(index).add(currentTime);
			}
			timeActivities.getActivities(index).add(valueInt);
		}
		for (IntValueHolder intValueHolder : intActivityValues) {
			needToFlush = needToFlush || intValueHolder.activityIntValues.update(index, valueStr);
		}
		return needToFlush;
	}

	@Override
	public void delete(int index) {
		timeActivities.reset(index);
	}

	@Override
	public Runnable prepareFlush() {
		final List<Runnable> flushes = new ArrayList<Runnable>(intActivityValues.length);
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
	  aggregatesUpdateJob.stop();	  
	  for (IntValueHolder intValueHolder : intActivityValues) {
			intValueHolder.activityIntValues.close();
		}
		
	}
	public static class IntValueHolder implements Comparable<IntValueHolder> {
		public final ActivityIntValues activityIntValues;
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
	public static class AggregatesMetadata {
    protected int lastUpdatedTime;
    protected File aggregatesFile;
    private AggregatesMetadata() {      
     
    }
    public static AggregatesMetadata init(String dirPath, String fieldName) {
      AggregatesMetadata ret = new AggregatesMetadata();
      File aggregatesFile = new File(dirPath, fieldName + ".aggregates");
      try {
      if (!aggregatesFile.exists()) {
        aggregatesFile.createNewFile();
        //minimum possible time
        ret.lastUpdatedTime = 0;
        FileUtils.writeStringToFile(aggregatesFile, String.valueOf(ret.lastUpdatedTime));
      } else {
        ret.lastUpdatedTime = Integer.parseInt(FileUtils.readFileToString(aggregatesFile));
      }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      ret.aggregatesFile = aggregatesFile;
      return ret;
    }
    public void updateTime(int currentTime) {
      lastUpdatedTime  = currentTime;
      try {
        FileUtils.writeStringToFile(aggregatesFile, String.valueOf(currentTime));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    public int getLastUpdatedTime() {
      return lastUpdatedTime;
    }
    
  }
	public static TimeAggregatedActivityValues valueOf(String fieldName, List<String> times, int count, String indexDirPath) {
	  TimeAggregatedActivityValues ret = new TimeAggregatedActivityValues(fieldName, times, count, indexDirPath);
	  ret.init(count > 0 ? count : 15000);
	  return ret;
	}
  public Map<String, ActivityIntValues> getValuesMap() {
    return valuesMap;
  }
	
}
