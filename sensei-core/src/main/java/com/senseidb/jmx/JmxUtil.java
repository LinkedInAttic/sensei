/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */
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

  /**
   * Unregister all MBeans that are registered through the
   * {@link #registerMBean(javax.management.StandardMBean, String, String)} method.
   */
  public static void unregisterMBeans(){
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
  
//  static{
//	  Runtime.getRuntime().addShutdownHook(new Thread(){
//		 public void run(){
//		   unregisterMBeans();
//		 }
//	  });
//  }
}
