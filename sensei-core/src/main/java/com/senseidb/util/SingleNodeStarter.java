package com.senseidb.util;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.mortbay.jetty.Server;

import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import com.senseidb.cluster.routing.SenseiPartitionedLoadBalancerFactory;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.conf.SenseiServerBuilder;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.node.SenseiServer;
import com.senseidb.search.node.broker.BrokerConfig;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;

public class SingleNodeStarter {
  private static boolean serverStarted = false;
  private static Server jettyServer;
  private static SenseiServer server;

  public static void start(String localPath, int expectedDocs) {
    start(new File(getUri(localPath)), expectedDocs);
  }

  public static void start(File confDir, int expectedDocs) {
    if (!serverStarted) {
      try {
        PropertiesConfiguration senseiConfiguration = new PropertiesConfiguration(new File(confDir, "sensei.properties"));
        final String indexDir = senseiConfiguration.getString(SenseiConfParams.SENSEI_INDEX_DIR);
       // rmrf(new File(indexDir));
        SenseiServerBuilder senseiServerBuilder = new SenseiServerBuilder(confDir, null);
        server = senseiServerBuilder.buildServer();
        jettyServer = senseiServerBuilder.buildHttpRestServer();
        server.start(true);
        jettyServer.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
            shutdown();
          }
        });
        PartitionedLoadBalancerFactory balancerFactory = new SenseiPartitionedLoadBalancerFactory(50);
        BrokerConfig brokerConfig = new BrokerConfig(senseiConfiguration, balancerFactory);
        brokerConfig.init();
        SenseiBroker senseiBroker = brokerConfig.buildSenseiBroker();
        while (true) {
          SenseiResult senseiResult = senseiBroker.browse(new SenseiRequest());
          int totalDocs = senseiResult.getTotalDocs();
          System.out.println("TotalDocs = " + totalDocs);
          if (totalDocs >= expectedDocs) {
            break;
          }
          Thread.sleep(100);
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
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
 
  public static boolean isServerStarted() {
    return serverStarted;
  }

  private static URI getUri(String localPath) {
    try {
      return SingleNodeStarter.class.getClassLoader().getResource(localPath).toURI();
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void shutdown() {
    try {
      jettyServer.stop();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      server.shutdown();
      serverStarted = false;
    }
  }
}
