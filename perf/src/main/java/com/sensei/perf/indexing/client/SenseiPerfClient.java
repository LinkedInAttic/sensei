package com.sensei.perf.indexing.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.DataConfiguration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.springframework.remoting.httpinvoker.HttpInvokerProxyFactoryBean;

import com.sensei.search.client.servlet.DefaultSenseiJSONServlet;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.svc.api.SenseiService;
import com.sensei.search.svc.impl.HttpRestSenseiServiceImpl;
import com.sensei.search.util.HttpUtil;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.TimerMetric;

public class SenseiPerfClient {

	public static final String URL = "sensei.client.url";
	public static final String TYPE = "sensei.client.type";
	public static final String JMX_URL = "sensei.jmx.url";
	public static final String TIME_TO_RUN = "sensei.perf.timeToRunMin";
	public static final String NUM_THREADS = "sensei.perf.numThreads";
	public static final String WAIT_INTERVAL = "sensei.perf.waitIntervalMs";
	public static final String REQ_FILE = "sensei.perf.request.file";
	
	public static final String SEARCH_REPORT_FILE = "search-report.txt";
	public static final String INDEXING_REPORT_FILE = "indexing-report.txt";
	public static final String SYSTEM_REPORT_FILE = "system-report.txt";
	
	static TimerMetric searchTimer = Metrics.newTimer(SenseiPerfClient.class, "perf-timer", TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS);
	private static PrintStream searchReport;
	private static PrintStream indexingReport;
	private static PrintStream systemReport;
	
	private static ExecutorService _threadPool;
	
	private static volatile int _numDocs  = 0;
	
	public static SenseiService buildSvc(String url,String type) throws Exception{
		if ("rest".equals(type)){
			HttpRestSenseiServiceImpl restSvc = new HttpRestSenseiServiceImpl(url);
			return restSvc;
		}
		else if ("sprintrpc".equals(type)){
			HttpInvokerProxyFactoryBean springInvokerBean = new HttpInvokerProxyFactoryBean();
		    springInvokerBean.setServiceUrl(url);
		    springInvokerBean.setServiceInterface(SenseiService.class);
		    springInvokerBean.afterPropertiesSet();
		    return (SenseiService)(springInvokerBean.getObject());
		}
		else{
			throw new IllegalArgumentException("type not supported: "+type);
		}
	}
	
	static String[] loadRequestFile(File reqFile) throws Exception{
		FileReader freader = null;
		try{
			freader = new FileReader(reqFile);
			BufferedReader reader = new BufferedReader(freader);
			LinkedList<String> list = new LinkedList<String>();
			while(true){
				String line = reader.readLine();
				if (line == null) break;
				
				int index = line.indexOf("q=");
				if (index >= 0){
					line = line.substring(index);
				}
				list.add(line);
			}
			
			return list.toArray(new String[0]);
		}
		finally{
			if (freader!=null){
				freader.close();
			}
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		File confDir;
		if (args.length<1){
			confDir = new File("perf-client-conf");
		}
		else{
			confDir = new File(args[0]);
		}
		
		System.out.println("Using conf dir: "+confDir.getAbsolutePath());
		org.apache.log4j.PropertyConfigurator.configure("log4j.properties");
		
		File confFile = new File(confDir,"perf-test.properties");


		System.out.println("Using conf file: "+confFile.getAbsolutePath());
		
		Configuration conf = new PropertiesConfiguration(confFile);
		
		String outDirString = conf.getString("output.dir",null);
		File outDir = outDirString == null ? confFile.getParentFile() : new File(outDirString);
		outDir.mkdirs();
		
		File searchFile = new File(outDir,SEARCH_REPORT_FILE);
		FileOutputStream searchFileOut = new FileOutputStream(searchFile);
		searchReport = new PrintStream(searchFileOut);
		
		
		File indexingFile = new File(outDir,INDEXING_REPORT_FILE);
		FileOutputStream indexingFileOut = new FileOutputStream(indexingFile);
		indexingReport = new PrintStream(indexingFileOut);
		
		
		File systemFile = new File(outDir,SYSTEM_REPORT_FILE);
		FileOutputStream systemFileOut = new FileOutputStream(systemFile);
		systemReport = new PrintStream(systemFileOut);
		
		String url = conf.getString(URL);
		String type = conf.getString(TYPE);
		
		SenseiService svc = buildSvc(url, type);
		
		int nThreads = conf.getInt(NUM_THREADS);
		
		_threadPool= Executors.newFixedThreadPool(nThreads+2);
		
		String reqFile = conf.getString(REQ_FILE);
		
		long waitInterval = conf.getLong(WAIT_INTERVAL,500);
		int timeToRun = conf.getInt(TIME_TO_RUN);
		
		String[] queries = loadRequestFile(new File(reqFile));
		
		
	    SearchThread[] searchThreads = new SearchThread[nThreads];
	    for (int i=0;i<nThreads;++i){
	    	searchThreads[i] = new SearchThread(url,type,queries,waitInterval,timeToRun);
	    }
	    
	    ArrayList<Future<?>> futureList = new ArrayList<Future<?>>(nThreads);
	    for (SearchThread searchThread :searchThreads){
		  //_threadPool..execute(searchThread);
	    	Future<?> future = _threadPool.submit(searchThread);
	    	futureList.add(future);
	    }
	    
	    SearchReporter searchReportThread = new SearchReporter(1000);
	    IndexReporter indexReportThread = new IndexReporter(1000);
	    
	    searchReportThread.start();
	    indexReportThread.start();
	    
	    for(Future<?> future : futureList){
	    	future.get();
	    }
	    
		
		_threadPool.shutdown();
	
		_threadPool.awaitTermination(10,TimeUnit.SECONDS);
	    
		System.out.println("test completed, cleaning up...");
		

	    searchReportThread.terminate();
	    indexReportThread.terminate();
	    
	    searchReportThread.join();
	    indexReportThread.join();
		
		svc.shutdown();
		
		searchReport.close();
		indexingReport.close();
		systemReport.close();
	}
	
	static class SearchReporter extends Thread{
		
		private volatile boolean  stop;
		private final long _tickInterval;
		public SearchReporter(long tickInterval){
			super("search-reporter");
			super.setDaemon(true);
			stop = false;
			_tickInterval = tickInterval;
		}
		
		public void terminate(){
			stop = true;
			Thread.currentThread().interrupt();
		}
		
		public void run(){
			while(!stop){
				StringBuilder builder = new StringBuilder();
				builder.append(System.currentTimeMillis()).append(",");
				builder.append(searchTimer.count()).append(",");
				builder.append(searchTimer.min()).append(",");
				builder.append(searchTimer.max()).append(",");
				builder.append(searchTimer.mean()).append(",");
				double[] percents = searchTimer.percentiles(90,95,99);
				builder.append(percents[0]).append(",");
				builder.append(percents[1]).append(",");
				builder.append(percents[2]);
				searchReport.println(builder.toString());
				searchReport.flush();
				try {
					Thread.sleep(_tickInterval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
private static class IndexReporter extends Thread{
		
		private volatile boolean  stop;
		private final long _tickInterval;
		public IndexReporter(long tickInterval){
			super("indexing-reporter");
			super.setDaemon(true);
			stop = false;
			_tickInterval = tickInterval;
		}
		
		public void terminate(){
			stop = true;
			Thread.currentThread().interrupt();
		}
		
		public void run(){
			while(!stop){
				StringBuilder builder = new StringBuilder();
				builder.append(System.currentTimeMillis()).append(",");
				builder.append(_numDocs);
				indexingReport.println(builder.toString());
				indexingReport.flush();
				try {
					Thread.sleep(_tickInterval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private static class SearchThread extends PollingDataCollector{

		private SenseiService _svc;
		private final String[] _queries;
		private final Random rand;
		
		public SearchThread(String url,String type,String[] queries,long waitInterval,int timeToRun){
		  super(waitInterval,timeToRun);
		  try{
		    _svc = SenseiPerfClient.buildSvc(url, type);
		    
		    // do a search 
		    SenseiResult res = _svc.doQuery(new SenseiRequest());
		    _numDocs = res.getTotalDocs();
		  }
		  catch(Exception e){
			_svc = null;
			e.printStackTrace();
		  }
		  _queries = queries;
		  rand = new Random(System.nanoTime());
		}
		
		@Override
		public void shutDown() {
			_svc.shutdown();
		}

		public void doWork() throws Exception{
			if (_svc==null){
				throw new Exception("null sensei service...");
			}
			int qid = rand.nextInt(_queries.length);
			   String q = _queries[qid];
			   Map reqMap = HttpUtil.buildRequestMap(q);			   
			   MapConfiguration mapConf = new MapConfiguration(reqMap);
			   DataConfiguration params = new DataConfiguration(mapConf);
			   final SenseiRequest req = DefaultSenseiJSONServlet.convertSenseiRequest(params);
			   try{
				   SenseiResult res = searchTimer.time(new Callable<SenseiResult>(){

					@Override
					public SenseiResult call() throws Exception {
						return  _svc.doQuery(req);
					}
				   });	
				   
				   _numDocs = res.getTotalDocs();
			   }
			   catch(Exception e){
			     e.printStackTrace();
			   }
		}
	}
	
	public static class SystemStatsCollector extends PollingDataCollector{
		private JMXMonitor _jmxMonitor;
		
		public SystemStatsCollector(String jmxUrl,long waitInterval,int timeToRun){
			super(waitInterval,timeToRun);
			_jmxMonitor = new JMXMonitor(jmxUrl);
		}
		
		
		
		@Override
		public void doWork() throws Exception {
			
		}



		@Override
		public void shutDown() {
			_jmxMonitor.shutdown();
		}
	}

}
