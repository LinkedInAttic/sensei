package com.senseidb.jmx;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.log4j.Logger;

import com.senseidb.metrics.MetricsConstants;

public class JmxUtil {

  private static final Logger log = Logger.getLogger(JmxUtil.class);
  private static final MBeanServer MbeanServer = ManagementFactory.getPlatformMBeanServer();
  private static final List<ObjectName> RegisteredBeans = Collections.synchronizedList(new LinkedList<ObjectName>());
  public static MBeanServer registerNewJmxServer(MBeanServer newBeanServer) {
    MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
    try {
    Field platformMBeanServerField = ManagementFactory.class.getDeclaredField("platformMBeanServer");
    platformMBeanServerField.setAccessible(true);
    platformMBeanServerField.set(null, newBeanServer);    
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    return platformMBeanServer;
  }
  
  public static void registerMBean(StandardMBean bean,String key,String val)
  {
    ObjectName objectName = null;
    try
    {
      log.info("registering jmx mbean: "+objectName);
      objectName = new ObjectName(MetricsConstants.Domain,key,val);
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
}
