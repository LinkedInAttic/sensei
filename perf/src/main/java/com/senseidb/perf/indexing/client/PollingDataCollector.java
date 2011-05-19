package com.senseidb.perf.indexing.client;


public abstract class PollingDataCollector implements Runnable {
	private final int _timeToRun;
	private final long _waitInterval;
	
	public PollingDataCollector(long waitInterval,int timeToRun){
		_timeToRun = timeToRun;
		_waitInterval = waitInterval;
	}
	
	public abstract void doWork() throws Exception;
	
	public void shutDown(){
		
	}
	
	@Override
	public void run() {
		long start = System.currentTimeMillis();
		   long now = System.currentTimeMillis();
		   long duration = _timeToRun*1000*60;
		   try{
		     while((now - start) < duration){
			   try{
				  doWork();
			   }
			   catch(Exception e){
			     e.printStackTrace();
			   }
			   try {
				Thread.sleep(_waitInterval);
			   } catch (InterruptedException e) {
				   e.printStackTrace();
				break;
			   }
			   now = System.currentTimeMillis();
		     }
		   }
		   finally{
			   shutDown();
		   }
	}
}
