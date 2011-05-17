package com.senseidb.perf.indexing.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.LinkedList;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.springframework.remoting.httpinvoker.HttpInvokerProxyFactoryBean;

import com.sensei.search.svc.api.SenseiService;
import com.sensei.search.svc.impl.HttpRestSenseiServiceImpl;

public class SenseiPerfClient {

	public static final String URL = "sense.client.url";
	public static final String TYPE = "sense.client.type";
	public static final String JMX_URL = "sense.jmx.url";
	public static final String TIME_TO_RUN = "sense.perf.timeToRunMin";
	public static final String NUM_THREADS = "sense.perf.numThreads";
	public static final String WAIT_INTERVAL = "sense.perf.waitIntervalMs";
	
	static SenseiService buildSvc(String url,String type) throws Exception{
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
		
		File confFile = new File(confDir,"perf-test.properties");

		Configuration conf = new PropertiesConfiguration(confFile);
		
		String url = conf.getString(URL);
		String type = conf.getString(TYPE);
		
		SenseiService svc = buildSvc(url, type);
		
		
		
		svc.shutdown();
	}
	
	private static class SearchThread implements Runnable{

		private final SenseiService _svc;
		private final long _waitInterval;
		private final int _timeToRun;
		
		SearchThread(SenseiService svc,long waitInterval,int timeToRun){
		  _svc = svc;
		  _waitInterval = waitInterval;
		  _timeToRun = timeToRun;
		}
		
		@Override
		public void run() {
		   long start = System.currentTimeMillis();
		   long now = System.currentTimeMillis();
		   long duration = _timeToRun*1000*60;
		   while((now - start) < duration){
			   
			   now = System.currentTimeMillis();
		   }
		}
		
	}

}
