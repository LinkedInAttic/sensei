package com.senseidb.indexing.activity.time;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.senseidb.indexing.activity.ActivityIntValues;
import com.senseidb.indexing.activity.ActivityValues;

public class TimeAggregatedActivityValues implements ActivityValues {
	private Map<String, Integer> times = new HashMap<String, Integer>();
	private final String fieldName;
	private Map<String, ActivityIntValues> valuesMap = new HashMap<String, ActivityIntValues>();
	private IntValueHolder[] intActivityValues;
	private TimeHitsHolder timeActivities;
	
	public TimeAggregatedActivityValues(String fieldName, List<String> times, int count, String indexDirPath) {
		this.fieldName = fieldName;
		intActivityValues = new IntValueHolder[times.size()];
		int index = 0;
		for(String time : times) {
			int timeInMinutes = extractTimeInMinutes(time);
			this.times.put(time, timeInMinutes);
			ActivityIntValues activityIntValues = ActivityIntValues.readFromFile(indexDirPath, fieldName + ":" + time, count);
			//TODO init timeAggregates
			this.valuesMap.put(time, activityIntValues);
			intActivityValues[index++] = new IntValueHolder(activityIntValues, time, timeInMinutes);
		}
		Arrays.sort(intActivityValues);
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
	}

	@Override
	public boolean update(int index, Object value) {
		boolean needToFlush = false;
		int valueInt = Integer.parseInt(value.toString());
		String valueStr = valueInt > 0 ? "+" + valueInt : String.valueOf(valueInt);
		synchronized (timeActivities.getLock(index)) {
			if (!timeActivities.isSet(index)) {
				timeActivities.setActivities(index, new IntContainer(1));
				timeActivities.setTime(index, new IntContainer(1));
			}
		}
		synchronized (timeActivities.getLock(index)) {
			timeActivities.getTimes(index).add(Clock.getCurrentTimeInMinutes());
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
		public int compareTo(IntValueHolder o) {
			return timeInMinutes -  o.timeInMinutes;
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
		private void ensureCapacity(int currentArraySize) {
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
	
}
