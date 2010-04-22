/**
 * 
 */
package com.sensei.search.server;

import java.io.File;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.google.protobuf.Message;
import com.linkedin.norbert.cluster.InvalidNodeException;
import com.linkedin.norbert.cluster.Node;
import com.linkedin.norbert.cluster.javaapi.ClusterClient;
import com.linkedin.norbert.network.NetworkingException;
import com.linkedin.norbert.network.javaapi.MessageHandler;
import com.linkedin.norbert.network.javaapi.NettyNetworkServer;
import com.linkedin.norbert.network.javaapi.NetworkServer;
import com.sensei.search.nodes.SenseiServerConfig;
import com.sensei.search.util.SenseiDefaults;

/**
 * @author nnarkhed
 *
 */
public class SenseiNetworkServer implements NetworkServer
{
  private NetworkServer _server;
 
  public SenseiNetworkServer(ClusterClient clusterClient)
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

    File confFile = new File(confDir, SenseiDefaults.SENSEI_SERVER_CONF_FILE);
    ApplicationContext springCtx = new FileSystemXmlApplicationContext("file:"+confFile.getAbsolutePath());
    SenseiServerConfig serverConfig = (SenseiServerConfig)springCtx.getBean("network-server-config");
    serverConfig.setClusterClient(clusterClient);
    _server = new NettyNetworkServer(serverConfig.getNetworkServerConfig());
  }
  
  public SenseiNetworkServer(String clusterName, String zooKeeperURL, int zooKeeperSessionTimeoutMillis)
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

    File confFile = new File(confDir, SenseiDefaults.SENSEI_SERVER_CONF_FILE);
    ApplicationContext springCtx = new FileSystemXmlApplicationContext("file:"+confFile.getAbsolutePath());
    SenseiServerConfig serverConfig = (SenseiServerConfig)springCtx.getBean("network-server-config");
    serverConfig.setServiceName(clusterName);
    serverConfig.setZooKeeperURL(zooKeeperURL);
    serverConfig.setZooKeeperSessionTimeout(zooKeeperSessionTimeoutMillis);
    _server = new NettyNetworkServer(serverConfig.getNetworkServerConfig());    
  }
  
  public void bind(int nodeid) throws InvalidNodeException,
      NetworkingException
  {
    _server.bind(nodeid);
  }

  public void bind(int nodeid, boolean markAvailable) throws InvalidNodeException,
      NetworkingException
  {
    _server.bind(nodeid, markAvailable);
  }

  public Node getMyNode()
  {
    return _server.getMyNode();
  }

  public void markAvailable()
  {
    _server.markAvailable();
  }

  public void markUnavailable()
  {
    _server.markUnavailable();
  }

  public void registerHandler(Message request, Message response, MessageHandler handler)
  {
    _server.registerHandler(request, response, handler);
  }

  public void shutdown()
  {
    _server.shutdown();
  }
  
}
