package com.sensei.conf;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient;
import com.linkedin.norbert.javacompat.network.NettyNetworkServer;
import com.linkedin.norbert.javacompat.network.NetworkServer;
import com.linkedin.norbert.javacompat.network.NetworkServerConfig;
import com.sensei.search.nodes.SenseiServer;

public class SenseiServerBuilder implements SenseiConfParams{

  public static final String SENSEI_PROPERTIES = "sensei.properties";
  public static final String CUSTOM_FACETS = "custom-facets.xml";
  public static final String SCHEMA_FILE = "schema.xml";
  public static final String PLUGINS = "plugins.xml";
  
  private final File _confDir;
  private final Configuration _senseiConf;
  private final ApplicationContext _pluginContext;
  private final ApplicationContext _customFacetContext;
  
  private static ApplicationContext loadSpringContext(File confFile){
	ApplicationContext springCtx = null;
	if (confFile.exists()){
	  springCtx = new FileSystemXmlApplicationContext("file:"+confFile.getAbsolutePath());
	}
	return springCtx;
  }
  
  private static ClusterClient buildClusterClient(Configuration conf){
	  String clusterName = conf.getString(SENSEI_CLUSTER_NAME);
	  String zkUrl = conf.getString(SENSEI_CLUSTER_URL);
	  int zkTimeout = conf.getInt(SENSEI_CLUSTER_TIMEOUT, 300000);
	  return new ZooKeeperClusterClient(clusterName, zkUrl,zkTimeout);
  }
  
  private static NetworkServer buildNetworkServer(Configuration conf,ClusterClient clusterClient){
	  NetworkServerConfig networkConfig = new NetworkServerConfig();
	  networkConfig.setClusterClient(clusterClient);
	  networkConfig.setRequestThreadCorePoolSize(conf.getInt(SERVER_REQ_THREAD_POOL_SIZE, 20));
	  networkConfig.setRequestThreadMaxPoolSize(conf.getInt(SERVER_REQ_THREAD_POOL_MAXSIZE,70));
	  networkConfig.setRequestThreadKeepAliveTimeSecs(conf.getInt(SERVER_REQ_THREAD_POOL_KEEPALIVE,300));
	  return new NettyNetworkServer(networkConfig);
  }
  
  public SenseiServerBuilder(File confDir) throws ConfigurationException{
	  _confDir = confDir;
	  File senseiConfFile = new File(confDir,SENSEI_PROPERTIES);
	  if (!senseiConfFile.exists()){
		throw new ConfigurationException("configuration file: "+senseiConfFile.getAbsolutePath()+" does not exist.");
	  }
	  _senseiConf = new PropertiesConfiguration(senseiConfFile);
	  _pluginContext = loadSpringContext(new File(confDir,PLUGINS));
	  _customFacetContext = loadSpringContext(new File(confDir,CUSTOM_FACETS));
  }
  
  public SenseiServer buildServer(){
	  int nodeid = _senseiConf.getInt(NODE_ID);
	  int port = _senseiConf.getInt(SERVER_PORT);
	  
	  ClusterClient clusterClient = buildClusterClient(_senseiConf);
	  NetworkServer networkServer = buildNetworkServer(_senseiConf,clusterClient);
	  File extDir = new File(_confDir,"ext");
	  return null;
  }
}
