package com.sensei.perf.indexing.client;

import java.io.IOException;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class JMXMonitor {
	private String _jmxUrl;
	private JMXConnector jmxc;
	private MBeanServerConnection mbsc;
	
	public JMXMonitor(String jmxUrl){
		_jmxUrl = jmxUrl;
		try{
		  JMXServiceURL url = new JMXServiceURL(_jmxUrl);
		  jmxc = JMXConnectorFactory.connect(url, null);
		  mbsc = jmxc.getMBeanServerConnection();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public MBeanServerConnection getMbeanServerConnection(){
		return mbsc;
	}
	
	public void shutdown(){
		if (jmxc!=null){
			try {
				jmxc.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
