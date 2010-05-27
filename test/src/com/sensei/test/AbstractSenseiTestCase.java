/**
 * 
 */
package com.sensei.test;

import java.io.File;

import junit.framework.TestCase;

import org.junit.Before;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.linkedin.norbert.cluster.javaapi.ClusterClient;
import com.linkedin.norbert.network.javaapi.NetworkServer;
import com.sensei.search.cluster.client.SenseiNetworkClient;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiResultBPO;

/**
 * @author nnarkhed
 *
 */
public class AbstractSenseiTestCase extends TestCase {

  protected SenseiNetworkClient networkClient;
  protected ClusterClient clusterClient;
  protected NetworkServer networkServer1;
  protected NetworkServer networkServer2;
  protected static final String SENSEI_TEST_CONF_FILE="sensei-test.spring";
  
  public AbstractSenseiTestCase(){
      super();
  }
  
  public AbstractSenseiTestCase(String name) {
    super(name);
  }

  @Before
  public void setUp() {
    // load the spring application context
    String confDirName=System.getProperty("test.conf.dir");
    File confDir = null;
    if (confDirName == null)
      confDir = new File("test/conf");
    else
      confDir = new File(confDirName);

    ApplicationContext testSpringCtx = new FileSystemXmlApplicationContext("file:" + new File(confDir, SENSEI_TEST_CONF_FILE).getAbsolutePath());
    networkClient = (SenseiNetworkClient)testSpringCtx.getBean("network-client");
    networkClient.registerRequest(SenseiRequestBPO.Request.getDefaultInstance(), SenseiResultBPO.Result.getDefaultInstance());
    clusterClient = (ClusterClient)testSpringCtx.getBean("cluster-client");
    networkServer1 = (NetworkServer)testSpringCtx.getBean("network-server-1");
    networkServer2 = (NetworkServer)testSpringCtx.getBean("network-server-2");
  }
  
}
