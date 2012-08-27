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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import com.sun.jmx.mbeanserver.SunJmxMBeanServer;

/**
 * Registers a custom platformMBeanServer, that is tolerable to registering severl MBeans with the same name. Instead of throwing the InstanceAlreadyExistsException 
 * it will try to add the numeric suffix to the ObjectName prior to registration
 * @author vzhabiuk
 *
 */
public class JmxSenseiMBeanServer {
  private static Logger logger = Logger.getLogger(JmxSenseiMBeanServer.class);
  private static boolean registered = false;

  public synchronized static void registerCustomMBeanServer() {
    try {
      if (!registered) {
        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();

        Field platformMBeanServerField = ManagementFactory.class.getDeclaredField("platformMBeanServer");
        platformMBeanServerField.setAccessible(true);
        Object modifiedMbeanServer = Proxy.newProxyInstance(platformMBeanServer.getClass().getClassLoader(),
            new Class[] { MBeanServer.class, SunJmxMBeanServer.class }, new MBeanServerInvocationHandler(
                platformMBeanServer));
        platformMBeanServerField.set(null, modifiedMbeanServer);
        registered = true;
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static class MBeanServerInvocationHandler implements InvocationHandler {
    final MBeanServer mBeanServer;

    public MBeanServerInvocationHandler(MBeanServer underlying) {
      this.mBeanServer = underlying;
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (!method.getName().equals("registerMBean")) {
        return method.invoke(mBeanServer, args);
      }
      ObjectName objectName = (ObjectName) args[1];
      String canonicalName = objectName.getCanonicalName();
      if (!canonicalName.contains(".sensei") && !canonicalName.contains(".linkedin") && !canonicalName.contains(".zoie") && !canonicalName.contains(".bobo")) {
        return method.invoke(mBeanServer, args);
      }
      for (int i = 0; i < 200; i++) {
        if (!mBeanServer.isRegistered(objectName)) {
          break;
        }
        logger.warn("The JMX bean with name [" + canonicalName + "] is already registered. Trying to add a numeric suffix to register the another one");
        objectName = new ObjectName(canonicalName + i);
      }
      args[1] = objectName;
      return method.invoke(mBeanServer, args);
    }
  }
}
