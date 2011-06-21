package com.sensei.search.jmx;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.yammer.metrics.core.TimerMetric;
import com.yammer.metrics.reporting.JmxReporter.Meter;
import com.yammer.metrics.reporting.JmxReporter.TimerMBean;

public class JmxUtil {

  private static final Logger log = Logger.getLogger(JmxUtil.class);
  private static final MBeanServer MbeanServer = ManagementFactory.getPlatformMBeanServer();
  private static final List<ObjectName> RegisteredBeans = Collections.synchronizedList(new LinkedList<ObjectName>());

  public static final String Domain = "com.senseidb";
  
  public static void registerMBean(Object bean,String key,String val){
	  try{
	    ObjectName oname = new ObjectName(Domain,key,val);
	    registerMBean(bean, oname);
	  }
	  catch(Exception e){
		log.error(e.getMessage(),e);
	  }
  }
  
  public static void registerMBean(Object bean,ObjectName name){
	  try{
		log.info("registering jmx mbean: "+name);
		MbeanServer.registerMBean(bean, name);
		RegisteredBeans.add(name);
	  }
	  catch(Exception e){
		log.error(e.getMessage(),e);
		if (e instanceof InstanceAlreadyExistsException){
		  RegisteredBeans.add(name);
		}
	  }
  }
  
  private static void unregisterMBeans(){
	  for (ObjectName mbeanName : RegisteredBeans){
	    try {
	      log.info("unregistering jmx mbean: "+mbeanName);
	      MbeanServer.unregisterMBean(mbeanName);
	    }catch (Exception e) {
	      log.error(e.getMessage(),e);
	    } 
	  }
	  RegisteredBeans.clear();
  }
  
  static{
	  Runtime.getRuntime().addShutdownHook(new Thread(){
		 public void run(){
		   unregisterMBeans(); 
		 }
	  });
  }
  
  public static class Timer extends Meter implements TimerMBean {
      private final TimerMetric metric;

      public Timer(TimerMetric metric, ObjectName objectName) {
          super(metric, objectName);
          this.metric = metric;
      }

      @Override
      public double get50thPercentile() {
          return metric.percentiles(0.5)[0];
      }

      @Override
      public TimeUnit getLatencyUnit() {
          return metric.durationUnit();
      }

      @Override
      public double getMin() {
          return metric.min();
      }

      @Override
      public double getMax() {
          return metric.max();
      }

      @Override
      public double getMean() {
          return metric.mean();
      }

      @Override
      public double getStdDev() {
          return metric.stdDev();
      }

      @Override
      public double get75thPercentile() {
          return metric.percentiles(0.75)[0];
      }

      @Override
      public double get95thPercentile() {
          return metric.percentiles(0.95)[0];
      }

      @Override
      public double get98thPercentile() {
          return metric.percentiles(0.98)[0];
      }

      @Override
      public double get99thPercentile() {
          return metric.percentiles(0.99)[0];
      }

      @Override
      public double get999thPercentile() {
          return metric.percentiles(0.999)[0];
      }

      @Override
      public List<?> values() {
          return metric.values();
      }
  }

}
