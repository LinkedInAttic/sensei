/**
 * 
 */
package com.sensei.search.cluster.client;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.linkedin.norbert.cluster.ClusterDisconnectedException;
import com.linkedin.norbert.cluster.ClusterListenerKey;
import com.linkedin.norbert.cluster.Node;
import com.linkedin.norbert.cluster.javaapi.ClusterClient;
import com.linkedin.norbert.cluster.javaapi.ClusterListener;
import com.linkedin.norbert.cluster.javaapi.InMemoryClusterClient;
import com.linkedin.norbert.cluster.javaapi.ZooKeeperClusterClient;
import com.sensei.search.nodes.SenseiClusterConfig;
import com.sensei.search.util.SenseiDefaults;

/**
 * @author nnarkhed
 *
 */
public class SenseiClusterClientImpl
{
  private final ClusterClient _clusterClient;
  
  public SenseiClusterClientImpl(boolean inMemory)
  {
    String confDirName=System.getProperty("conf.dir");
    File confDir = null;
    if (confDirName == null)
    {
      confDir = new File("node-conf");
    }
    else
    {
      confDir = new File(confDirName);
    }

    File confFile = new File(confDir, SenseiDefaults.SENSEI_CLUSTER_CONF_FILE);
    ApplicationContext springCtx = new FileSystemXmlApplicationContext("file:"+confFile.getAbsolutePath());
    SenseiClusterConfig clusterConfig = (SenseiClusterConfig)springCtx.getBean("cluster-config");

    if(inMemory)
      _clusterClient = new InMemoryClusterClient(clusterConfig.getClusterName());
    else
    {
      _clusterClient = new ZooKeeperClusterClient(clusterConfig.getClusterName(), clusterConfig.getZooKeeperURL(),
                                                  clusterConfig.getZooKeeperSessionTimeoutMillis());                 
    }
  }
  
  public SenseiClusterClientImpl(String clusterName, boolean inMemory)
  {
    if(inMemory)
      _clusterClient = new InMemoryClusterClient(clusterName);
    else
    {
      String confDirName=System.getProperty("conf.dir");
      File confDir = null;
      if (confDirName == null)
      {
        confDir = new File("node-conf");
      }
      else
      {
        confDir = new File(confDirName);
      }

      File confFile = new File(confDir, SenseiDefaults.SENSEI_CLUSTER_CONF_FILE);
      ApplicationContext springCtx = new FileSystemXmlApplicationContext("file:"+confFile.getAbsolutePath());
      SenseiClusterConfig clusterConfig = (SenseiClusterConfig)springCtx.getBean("cluster-config");

      _clusterClient = new ZooKeeperClusterClient(clusterConfig.getClusterName(), clusterConfig.getZooKeeperURL(),
                                                         clusterConfig.getZooKeeperSessionTimeoutMillis());           
    }
  }

  public SenseiClusterClientImpl(File confFile, boolean inMemory)
  {
    ApplicationContext springCtx = new FileSystemXmlApplicationContext("file:"+confFile.getAbsolutePath());
    SenseiClusterConfig clusterConfig = (SenseiClusterConfig)springCtx.getBean("cluster-config");
    if(!inMemory)
    {
      _clusterClient = new ZooKeeperClusterClient(clusterConfig.getClusterName(), clusterConfig.getZooKeeperURL(),
                                                  clusterConfig.getZooKeeperSessionTimeoutMillis());
    }
    else
      _clusterClient = new InMemoryClusterClient(clusterConfig.getClusterName());
  }

  public SenseiClusterClientImpl(SenseiClusterConfig clusterConfig, boolean inMemory)
  {
    if(!inMemory)
    {
      _clusterClient = new ZooKeeperClusterClient(clusterConfig.getClusterName(), clusterConfig.getZooKeeperURL(),
                                                  clusterConfig.getZooKeeperSessionTimeoutMillis());
    }
    else
      _clusterClient = new InMemoryClusterClient(clusterConfig.getClusterName());      
  }
  
  public SenseiClusterClientImpl(String clusterName, String zooKeeperURL, int zooKeeperSessionTimeout, boolean inMemory)
  {
    if(!inMemory)
      _clusterClient = new ZooKeeperClusterClient(clusterName, zooKeeperURL, zooKeeperSessionTimeout);
    else
      _clusterClient = new InMemoryClusterClient(clusterName);
  }
  

  public ClusterClient getClusterClient()
  {
    return _clusterClient;
  }

//  public ClusterListenerKey addListener(ClusterListener listener)
//  {
//    return _clusterClient.addListener(listener);
//  }
//
//  public Node addNode(int id, String name) throws ClusterDisconnectedException
//  {
//    return _clusterClient.addNode(id, name);
//  }
//
//  public Node addNode(int id, String name, int[] partitions) throws ClusterDisconnectedException
//  {
//    return _clusterClient.addNode(id, name, partitions);
//  }
//
//  public void awaitConnection() throws InterruptedException
//  {
//    _clusterClient.awaitConnection();
//  }
//
//  public boolean awaitConnection(long time, TimeUnit unit) throws InterruptedException
//  {
//    return _clusterClient.awaitConnection(time, unit);
//  }
//
//  public void awaitConnectionUninterruptibly()
//  {
//    _clusterClient.awaitConnectionUninterruptibly();
//  }
//
//  public Node getNodeWithId(int id) throws ClusterDisconnectedException
//  {
//    return _clusterClient.getNodeWithId(id);
//  }
//
//  public Node[] getNodes() throws ClusterDisconnectedException
//  {
//    return _clusterClient.getNodes();
//  }
//
//  public String getServiceName()
//  {
//    return _clusterClient.getServiceName();
//  }
//
//  public boolean isConnected()
//  {
//    return _clusterClient.isConnected();
//  }
//
//  public boolean isShutdown()
//  {
//    return _clusterClient.isShutdown();
//  }
//
//  public void markNodeAvailable(int id) throws ClusterDisconnectedException
//  {
//    _clusterClient.markNodeAvailable(id);
//  }
//
//  public void markNodeUnavailable(int id) throws ClusterDisconnectedException
//  {
//    _clusterClient.markNodeUnavailable(id);
//  }
//
//  public void removeListener(ClusterListenerKey listenerKey)
//  {
//    _clusterClient.removeListener(listenerKey);
//  }
//
//  public void removeNode(int id) throws ClusterDisconnectedException
//  {
//    _clusterClient.removeNode(id);
//  }
//
//  public void shutdown()
//  {
//    _clusterClient.shutdown();
//  }
}
