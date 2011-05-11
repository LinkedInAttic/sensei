/**
 * 
 */
package com.sensei.test;

import java.io.File;
import java.util.Properties;

import javax.management.InstanceAlreadyExistsException;

import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.network.NetworkServer;
import com.sensei.search.cluster.client.SenseiNetworkClient;
import com.sensei.search.nodes.SenseiZoieFactory;

/**
 * @author nnarkhed
 *
 */
public class AbstractSenseiTestCase extends TestCase {
  private static final Logger logger = Logger.getLogger(AbstractSenseiTestCase.class);
  protected static SenseiNetworkClient networkClient;
  protected static ClusterClient clusterClient;
  protected static PartitionedLoadBalancerFactory<Integer> loadBalancerFactory;
  protected static NetworkServer networkServer1;
  protected static NetworkServer networkServer2;
  protected static final String SENSEI_TEST_CONF_FILE="sensei-test.spring";
  protected static SenseiZoieFactory<?> _zoieFactory;
  
  public AbstractSenseiTestCase(){
      super();
  }
  
  public AbstractSenseiTestCase(String name) {
    super(name);
  }
  static
  {
    try
    {
      org.apache.log4j.PropertyConfigurator.configure("resources/log4j.properties");
    } catch (Exception e)
    {
      org.apache.log4j.PropertyConfigurator.configure((Properties) null);
    }
    // load the spring application context
    String confDirName=System.getProperty("test.conf.dir");
    File confDir = null;
    if (confDirName == null)
      confDir = new File("src/test/conf");
    else
      confDir = new File(confDirName);

    ApplicationContext testSpringCtx = null;
    try
    {
      testSpringCtx = new FileSystemXmlApplicationContext("file:" + new File(confDir, SENSEI_TEST_CONF_FILE).getAbsolutePath());
    } catch(Throwable e)
    {
      if (e instanceof InstanceAlreadyExistsException) logger.warn("norbert JMX InstanceAlreadyExistsException");
      else logger.error("Unexpected Exception", e.getCause());
    }


    loadBalancerFactory = (PartitionedLoadBalancerFactory<Integer>) testSpringCtx.getBean("router-factory");
    clusterClient = (ClusterClient)testSpringCtx.getBean("cluster-client");
    networkClient = (SenseiNetworkClient)testSpringCtx.getBean("network-client");
    networkServer1 = (NetworkServer)testSpringCtx.getBean("network-server-1");
    networkServer2 = (NetworkServer)testSpringCtx.getBean("network-server-2");
    _zoieFactory = (SenseiZoieFactory<?>)testSpringCtx.getBean("zoie-system-factory");
  }

  public void testNothing(){
	
  }
}
