package com.senseidb.indexing.activity.time;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import com.senseidb.indexing.activity.time.TimeAggregatedActivityValues.TimeHitsHolder;

public class AggregatesUpdateJob implements Runnable {
	private final TimeHitsHolder timeActivities;
	private AtomicInteger lastUpdatedTime;
public AggregatesUpdateJob(TimeHitsHolder timeActivities, File updateMetadataFile) {
	this.timeActivities = timeActivities;
	// TODO Auto-generated constructor stub
}
@Override
public void run() {
	// TODO Auto-generated method stub
	
}
}
