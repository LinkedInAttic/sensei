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