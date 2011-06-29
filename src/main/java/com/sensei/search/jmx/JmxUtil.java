package com.sensei.search.jmx;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.log4j.Logger;

import com.yammer.metrics.core.CounterMetric;
import com.yammer.metrics.core.GaugeMetric;
import com.yammer.metrics.core.HistogramMetric;
import com.yammer.metrics.core.MeterMetric;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.TimerMetric;
import com.yammer.metrics.reporting.JmxReporter.Counter;
import com.yammer.metrics.reporting.JmxReporter.CounterMBean;
import com.yammer.metrics.reporting.JmxReporter.Gauge;
import com.yammer.metrics.reporting.JmxReporter.GaugeMBean;
import com.yammer.metrics.reporting.JmxReporter.HistogramMBean;
import com.yammer.metrics.reporting.JmxReporter.Meter;
import com.yammer.metrics.reporting.JmxReporter.MeterMBean;
import com.yammer.metrics.reporting.JmxReporter.TimerMBean;

public class JmxUtil {

  private static final Logger log = Logger.getLogger(JmxUtil.class);
  private static final MBeanServer MbeanServer = ManagementFactory.getPlatformMBeanServer();
  private static final List<ObjectName> RegisteredBeans = Collections.synchronizedList(new LinkedList<ObjectName>());

  public static final String Domain = "com.senseidb";
  
  public static void registerMBean(StandardMBean bean,String key,String val)
  {
    ObjectName objectName = null;
    try
    {
      log.info("registering jmx mbean: "+objectName);
      objectName = new ObjectName(Domain,key,val);
      MbeanServer.registerMBean(bean, objectName);
      RegisteredBeans.add(objectName);
    }
    catch(Exception e)
    {
      log.error(e.getMessage(),e);
      if (e instanceof InstanceAlreadyExistsException)
      {
        RegisteredBeans.add(objectName);
      }
    }
  }
  
  public static void registerMBean(Metric metric, ObjectName objectName){
    try
    {
      log.info("registering jmx mbean using a metric: "+objectName);
      if (metric instanceof GaugeMetric)
      {
        MbeanServer.registerMBean(new StandardMBean(new Gauge((GaugeMetric<?>) metric, objectName), GaugeMBean.class), objectName);
      }
      else if (metric instanceof CounterMetric)
      {
        MbeanServer.registerMBean(new StandardMBean(new Counter((CounterMetric) metric, objectName), CounterMBean.class), objectName);
      }
      else if (metric instanceof HistogramMetric)
      {
        MbeanServer.registerMBean(new StandardMBean(new Histogram((HistogramMetric) metric, objectName), HistogramMBean.class), objectName);
      }
      else if (metric instanceof MeterMetric)
      {
        MbeanServer.registerMBean(new StandardMBean(new Meter((MeterMetric) metric, objectName), MeterMBean.class), objectName);
      }
      else if (metric instanceof TimerMetric)
      {
        MbeanServer.registerMBean(new StandardMBean(new Timer((TimerMetric) metric, objectName), TimerMBean.class), objectName);
      }
      RegisteredBeans.add(objectName);
    }
    catch(Exception e)
    {
      log.error(e.getMessage(),e);
      if (e instanceof InstanceAlreadyExistsException)
      {
        RegisteredBeans.add(objectName);
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
  
  // XXX Copy JmxReporter.Timer and JmxReporter.Histogram here and make them
  // static.  These two classes should be removed once the fix in metric
  // is available.
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

  // XXX To be removed in the future.
  public static class Histogram implements HistogramMBean {
    private final ObjectName objectName;
    private final HistogramMetric metric;

    public Histogram(HistogramMetric metric, ObjectName objectName) {
      this.metric = metric;
      this.objectName = objectName;
    }

    @Override
    public ObjectName objectName() {
      return objectName;
    }

    @Override
    public double get50thPercentile() {
      return metric.percentiles(0.5)[0];
    }

    @Override
    public long getCount() {
      return metric.count();
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
