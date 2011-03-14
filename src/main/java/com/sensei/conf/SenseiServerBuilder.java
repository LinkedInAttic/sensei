package com.sensei.conf;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.network.NettyNetworkServer;
import com.linkedin.norbert.javacompat.network.NetworkServer;
import com.linkedin.norbert.javacompat.network.NetworkServerConfig;
import com.sensei.search.nodes.SenseiServer;

public class SenseiServerBuilder {

  public static final String SENSEI_PROPERTIES = "sensei.properties";
  public static final String CUSTOM_FACETS = "custom-facets.xml";
  public static final String SCHEMA_FILE = "schema.xml";
  public static final String PLUGINS = "plugins.xml";
  
  private static ApplicationContext loadSpringContext(File confFile){
	ApplicationContext springCtx = null;
	if (confFile.exists()){
	  springCtx = new FileSystemXmlApplicationContext("file:"+confFile.getAbsolutePath());
	}
	return springCtx;
  }
  
  private static NetworkServer buildNetworkServer(Configuration conf,ClusterClient clusterClient) throws ConfigurationException{
	  NetworkServerConfig networkConfig = new NetworkServerConfig();
	  networkConfig.setClusterClient(clusterClient);
	  //networkConfig.setRequestThreadCorePoolSize();
	  //networkConfig.
	  return new NettyNetworkServer(networkConfig);
  }
  
  public SenseiServerBuilder(File confDir) throws ConfigurationException{
	  File senseiConfFile = new File(confDir,SENSEI_PROPERTIES);
	  if (!senseiConfFile.exists()){
		throw new ConfigurationException("configuration file: "+senseiConfFile.getAbsolutePath()+" does not exist.");
	  }
	  Configuration senseiConf = new PropertiesConfiguration(senseiConfFile);
	  ApplicationContext pluginContext = loadSpringContext(new File(confDir,PLUGINS));
	  ApplicationContext customFacetContext = loadSpringContext(new File(confDir,CUSTOM_FACETS));
  }
  
  public SenseiServer buildServer(){
	  return null;
  }
}
