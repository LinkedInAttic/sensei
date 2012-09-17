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

import java.io.ObjectInputStream;
import java.util.Set;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.InvalidAttributeValueException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.OperationsException;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.loading.ClassLoaderRepository;

public class MockJMXServer implements MBeanServer {

  @Override
  public ObjectInstance createMBean(String className, ObjectName name) throws ReflectionException, InstanceAlreadyExistsException,
      MBeanRegistrationException, MBeanException, NotCompliantMBeanException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName) throws ReflectionException,
      InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException, InstanceNotFoundException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name, Object[] params, String[] signature) throws ReflectionException,
      InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName, Object[] params, String[] signature)
      throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException,
      InstanceNotFoundException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectInstance registerMBean(Object object, ObjectName name) throws InstanceAlreadyExistsException, MBeanRegistrationException,
      NotCompliantMBeanException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void unregisterMBean(ObjectName name) throws InstanceNotFoundException, MBeanRegistrationException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public ObjectInstance getObjectInstance(ObjectName name) throws InstanceNotFoundException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp query) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<ObjectName> queryNames(ObjectName name, QueryExp query) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isRegistered(ObjectName name) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Integer getMBeanCount() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object getAttribute(ObjectName name, String attribute) throws MBeanException, AttributeNotFoundException,
      InstanceNotFoundException, ReflectionException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public AttributeList getAttributes(ObjectName name, String[] attributes) throws InstanceNotFoundException, ReflectionException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setAttribute(ObjectName name, Attribute attribute) throws InstanceNotFoundException, AttributeNotFoundException,
      InvalidAttributeValueException, MBeanException, ReflectionException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public AttributeList setAttributes(ObjectName name, AttributeList attributes) throws InstanceNotFoundException, ReflectionException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object invoke(ObjectName name, String operationName, Object[] params, String[] signature) throws InstanceNotFoundException,
      MBeanException, ReflectionException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getDefaultDomain() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String[] getDomains() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void addNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter, Object handback)
      throws InstanceNotFoundException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void addNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter, Object handback)
      throws InstanceNotFoundException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void removeNotificationListener(ObjectName name, ObjectName listener) throws InstanceNotFoundException, ListenerNotFoundException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void removeNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter, Object handback)
      throws InstanceNotFoundException, ListenerNotFoundException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void removeNotificationListener(ObjectName name, NotificationListener listener) throws InstanceNotFoundException,
      ListenerNotFoundException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void removeNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter, Object handback)
      throws InstanceNotFoundException, ListenerNotFoundException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public MBeanInfo getMBeanInfo(ObjectName name) throws InstanceNotFoundException, IntrospectionException, ReflectionException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isInstanceOf(ObjectName name, String className) throws InstanceNotFoundException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Object instantiate(String className) throws ReflectionException, MBeanException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object instantiate(String className, ObjectName loaderName) throws ReflectionException, MBeanException, InstanceNotFoundException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object instantiate(String className, Object[] params, String[] signature) throws ReflectionException, MBeanException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object instantiate(String className, ObjectName loaderName, Object[] params, String[] signature) throws ReflectionException,
      MBeanException, InstanceNotFoundException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectInputStream deserialize(ObjectName name, byte[] data) throws InstanceNotFoundException, OperationsException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectInputStream deserialize(String className, byte[] data) throws OperationsException, ReflectionException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectInputStream deserialize(String className, ObjectName loaderName, byte[] data) throws InstanceNotFoundException,
      OperationsException, ReflectionException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ClassLoader getClassLoaderFor(ObjectName mbeanName) throws InstanceNotFoundException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ClassLoader getClassLoader(ObjectName loaderName) throws InstanceNotFoundException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ClassLoaderRepository getClassLoaderRepository() {
    // TODO Auto-generated method stub
    return null;
  }

}
