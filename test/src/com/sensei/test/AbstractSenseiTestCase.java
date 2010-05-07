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
  protected static final String SENSEI_NETWORK_CONF_FILE="sensei-test-network.spring";
  
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

//    ApplicationContext clusterSpringCtx = new FileSystemXmlApplicationContext("file:" + new File(confDir, SenseiDefaults.SENSEI_CLUSTER_CONF_FILE).getAbsolutePath());
    ApplicationContext networkSpringCtx = new FileSystemXmlApplicationContext("file:" + new File(confDir, SENSEI_NETWORK_CONF_FILE).getAbsolutePath());
    networkClient = (SenseiNetworkClient)networkSpringCtx.getBean("network-client");
    networkClient.registerRequest(SenseiRequestBPO.Request.getDefaultInstance(), SenseiResultBPO.Result.getDefaultInstance());
    clusterClient = (ClusterClient)networkSpringCtx.getBean("in-memory-cluster-client");
  }
  
}
