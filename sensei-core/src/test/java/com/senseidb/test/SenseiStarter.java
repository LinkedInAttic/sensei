package com.senseidb.test;

import java.io.File;
import java.net.URL;

import javax.management.InstanceAlreadyExistsException;

import com.senseidb.indexing.activity.facet.ActivityRangeFacetHandler;
import org.apache.log4j.Logger;
import org.mortbay.jetty.Server;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.network.NetworkServer;
import com.senseidb.cluster.client.SenseiNetworkClient;
import com.senseidb.conf.SenseiServerBuilder;
import com.senseidb.jmx.JmxSenseiMBeanServer;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.node.SenseiRequestScatterRewriter;
import com.senseidb.search.node.SenseiServer;
import com.senseidb.search.node.SenseiZoieFactory;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.svc.api.SenseiService;
import com.senseidb.svc.impl.HttpRestSenseiServiceImpl;

/**
 * Embeds all the logic for starting the test Sensei instance
 *
 */
public class SenseiStarter {
  private static final Logger logger = Logger.getLogger(SenseiStarter.class);

  public static File ConfDir1 = null;
  public static File ConfDir2 = null;
 

  public static File IndexDir = new File("sensei-index-test");
  public static URL SenseiUrl = null;
  public static SenseiBroker broker = null;
  public static SenseiService httpRestSenseiService = null;
  public static SenseiServer node1;
  public static SenseiServer node2;
  public static Server httpServer1;
  public static Server httpServer2;
  public static SenseiNetworkClient networkClient;
  public static ClusterClient clusterClient;
  public static SenseiRequestScatterRewriter requestRewriter;
  public static NetworkServer networkServer1;
  public static NetworkServer networkServer2;
  public static final String SENSEI_TEST_CONF_FILE="sensei-test.spring";
  public static SenseiZoieFactory<?> _zoieFactory;
  public static boolean started = false;

   public static URL  federatedBrokerUrl;
  

  /**
   * Will start the new Sensei instance once per process
   */
  public static synchronized void start(String confDir1, String confDir2) {
    ActivityRangeFacetHandler.isSynchronized = true;
    if (started) {
      logger.warn("The server had been already started");
      return;
    }
    try {
      JmxSenseiMBeanServer.registerCustomMBeanServer();
    ConfDir1 = new File(SenseiStarter.class.getClassLoader().getResource(confDir1).toURI());

    ConfDir2 = new File(SenseiStarter.class.getClassLoader().getResource(confDir2).toURI());
    org.apache.log4j.PropertyConfigurator.configure("resources/log4j.properties");
    loadFromSpringContext();
    boolean removeSuccessful = rmrf(IndexDir);
    if (!removeSuccessful) {
      throw new IllegalStateException("The index dir " + IndexDir + " coulnd't be purged");
    }
    SenseiServerBuilder senseiServerBuilder1 = null;
    senseiServerBuilder1 = new SenseiServerBuilder(ConfDir1, null);
    node1 = senseiServerBuilder1.buildServer();
    httpServer1 = senseiServerBuilder1.buildHttpRestServer();
    logger.info("Node 1 created.");
    SenseiServerBuilder senseiServerBuilder2 = null;
    senseiServerBuilder2 = new SenseiServerBuilder(ConfDir2, null);
    node2 = senseiServerBuilder2.buildServer();
    httpServer2 = senseiServerBuilder2.buildHttpRestServer();
    logger.info("Node 2 created.");
    broker = null;
    try
    {
      broker = new SenseiBroker(networkClient, clusterClient, true);
    } catch (NorbertException ne) {
      logger.info("shutting down cluster...", ne);
        clusterClient.shutdown();
        throw ne;
    }
    httpRestSenseiService = new HttpRestSenseiServiceImpl("http", "localhost", 8079, "/sensei");
    logger.info("Cluster client started");
    Runtime.getRuntime().addShutdownHook(new Thread(){
      @Override
      public void run(){
        shutdownSensei();
    }});
    node1.start(true);
    httpServer1.start();
    logger.info("Node 1 started");
    node2.start(true);
    httpServer2.start();
    logger.info("Node 2 started");
    SenseiUrl =  new URL("http://localhost:8079/sensei");
    federatedBrokerUrl =  new URL("http://localhost:8079/sensei/federatedBroker/");
    waitTillServerStarts();
    } catch (Throwable ex) {
      logger.error("Could not start the sensei", ex);
      throw new RuntimeException(ex);
    }finally {
      started = true;
    }
  }

  private static void waitTillServerStarts() throws Exception {

    SenseiRequest req = new SenseiRequest();
    SenseiResult res = null;
    int count = 0;
    do
    {
      Thread.sleep(500);
      res = broker.browse(req);
      System.out.println(""+res.getNumHits()+" loaded...");
      ++count;
    } while (count < 20 && res.getNumHits() < 15000);

  }

  private static void loadFromSpringContext() {

    ApplicationContext testSpringCtx = null;
    try
    {
      testSpringCtx = new ClassPathXmlApplicationContext("test-conf/sensei-test.spring");
    } catch(Throwable e)
    {
      if (e instanceof InstanceAlreadyExistsException)
        logger.warn("norbert JMX InstanceAlreadyExistsException");
      else
        logger.error("Unexpected Exception", e.getCause());
    }
    networkClient = (SenseiNetworkClient)testSpringCtx.getBean("network-client");
    clusterClient = (ClusterClient)testSpringCtx.getBean("cluster-client");
    requestRewriter = (SenseiRequestScatterRewriter)testSpringCtx.getBean("request-rewriter");
    networkServer1 = (NetworkServer)testSpringCtx.getBean("network-server-1");
    networkServer2 = (NetworkServer)testSpringCtx.getBean("network-server-2");
    _zoieFactory = (SenseiZoieFactory<?>)testSpringCtx.getBean("zoie-system-factory");
  }

  public static boolean rmrf(File f) {
    if (f == null || !f.exists()) {
      return true;
    }

    if (f.isDirectory()) {
      for (File sub : f.listFiles()) {
        if (!rmrf(sub))
          return false;
      }
    }
    return f.delete();
  }

  public static void shutdownSensei() {
    try{ broker.shutdown();}catch(Throwable t){}
    try{ httpRestSenseiService.shutdown();}catch(Throwable t){}
    try{node1.shutdown();}catch(Throwable t){}
    try{httpServer1.stop();}catch(Throwable t){}
    try{node2.shutdown();}catch(Throwable t){}
    try{httpServer2.stop();}catch(Throwable t){}
    try{networkClient.shutdown();}catch(Throwable t){}
    try{clusterClient.shutdown();}catch(Throwable t){}
    rmrf(IndexDir);
    started = false;
  }

}
