package com.sensei.search.nodes;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.mbean.ZoieIndexingStatusAdmin;
import proj.zoie.mbean.ZoieIndexingStatusAdminMBean;
import proj.zoie.mbean.ZoieSystemAdminMBean;

import com.browseengine.bobo.api.BoboIndexReader;
import com.linkedin.norbert.cluster.javaapi.ClusterClient;
import com.sensei.search.cluster.client.ClusterClientFactory;
import com.sensei.search.cluster.client.SenseiClusterClientImpl;
import com.sensei.search.svc.api.SenseiException;

public class SenseiServer {
	private static final Logger logger = Logger.getLogger(SenseiServer.class);
	private static final String DEFAULT_CONF_FILE = "sensei-node.spring";
    private static final String AVAILABLE = "available";
    private static final String UNAVAILABLE = "unavailable";	
	
    private int _id;
    private int _port;
    private int[] _partitions;
    private String _partitionString;
    private File _confDir;
    private SenseiNode _node;
    
    private SenseiServer(int id, int port, int[] partitions, File confDir)
    {
      _id = id;
      _port = port;
      _partitions = partitions;
      StringBuffer sb = new StringBuffer();
      if(partitions.length > 0) sb.append(String.valueOf(partitions[0]));
      for(int i = 1; i < partitions.length; i++)
      {
        sb.append(',');
        sb.append(String.valueOf(partitions[i]));
      }
      _partitionString = sb.toString();
      _confDir = confDir;
    }
    
	private static String help(){
		StringBuffer buffer = new StringBuffer();
		buffer.append("Usage: [id] [port] [partitions] [conf.dir] [availability]\n");
		buffer.append("====================================\n");
		buffer.append("id - node id (integer), required\n");
		buffer.append("port - server port (integer), required\n");
		buffer.append("partitions - comma separated list of partition numbers this node can serve, required\n");
        buffer.append("conf.dir - server configuration directory, required\n");
        buffer.append("availability - \"available\" or \"unavailable\", optional default is \"available\"\n");
		buffer.append("====================================\n");
		return buffer.toString();
	}
	
	private void start(boolean available) throws Exception
	{
	    File confFile = new File(_confDir,DEFAULT_CONF_FILE);
		File extDir = new File(_confDir,"ext");
		File[] jarfiles = extDir.listFiles(new FilenameFilter(){

			public boolean accept(File dir, String name) {
				return name.endsWith(".jar");
			}
		});
		

		
		if (jarfiles!=null && jarfiles.length > 0){
		  URL[] jarURLs = new URL[jarfiles.length];
		  ClassLoader parentLoader = Thread.currentThread().getContextClassLoader();
		  for (int i=0;i<jarfiles.length;++i){
			jarURLs[i] = new URL("jar:file://" + jarfiles[i].getAbsolutePath() + "!/");  
		  }
		  URLClassLoader classloader = new URLClassLoader(jarURLs,parentLoader);
		  Thread.currentThread().setContextClassLoader(classloader);
		}
		
		ApplicationContext springCtx = new FileSystemXmlApplicationContext("file:"+confFile.getAbsolutePath());
		
		// get config parameters
//		SenseiClusterConfig clusterConfig = (SenseiClusterConfig)springCtx.getBean("cluster-config");
		ClusterClient clusterClient = ClusterClientFactory.newInstance().newZookeeperClient();
        String clusterName = clusterClient.getServiceName();
//        String zookeeperURL = clusterConfig.getZooKeeperURL();
//        int zookeeperTimeout = clusterConfig.getZooKeeperSessionTimeoutMillis();
        
        logger.info("ClusterName: " + clusterName);
        logger.info("Cluster info: " + clusterClient.toString());
//        logger.info("Zookeeper timeout: " + zookeeperTimeout);
        
        Map<Integer,SenseiQueryBuilderFactory> builderFactoryMap = new HashMap<Integer, SenseiQueryBuilderFactory>();
		SenseiZoieSystemFactory<?> zoieSystemFactory = (SenseiZoieSystemFactory<?>)springCtx.getBean("zoie-system-factory");
		SenseiIndexLoaderFactory indexLoaderFactory = (SenseiIndexLoaderFactory)springCtx.getBean("index-loader-factory");
        
		Map<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> readerFactoryMap = 
				new HashMap<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>>();
		
        final HashSet<ZoieSystem<BoboIndexReader,?>> zoieSystems = new HashSet<ZoieSystem<BoboIndexReader,?>>();
        final HashSet<SenseiIndexLoader> indexLoaders = new HashSet<SenseiIndexLoader>();
		
        MBeanServer mbeanServer = java.lang.management.ManagementFactory.getPlatformMBeanServer();
        
		for (int part : _partitions){
		  //in simple case query builder is the same for each partition
		  builderFactoryMap.put(part, (SenseiQueryBuilderFactory)springCtx.getBean("query-builder-factory"));
			
		  ZoieSystem<BoboIndexReader,?> zoieSystem = zoieSystemFactory.getZoieSystem(part);
		  
		  // register ZoieSystemAdminMBean
		  mbeanServer.registerMBean(new StandardMBean(zoieSystem.getAdminMBean(), ZoieSystemAdminMBean.class),
		                            new ObjectName(clusterName, "name", "zoie-system-" + part));
		  // register ZoieIndexingStatusAdminMBean
		  mbeanServer.registerMBean(new StandardMBean(new ZoieIndexingStatusAdmin(zoieSystem), ZoieIndexingStatusAdminMBean.class),
		                            new ObjectName(clusterName, "name", "zoie-indexing-status-" + part));
	          	  
		  
		  if(!zoieSystems.contains(zoieSystem))
		  {
		    zoieSystem.start();
		    zoieSystems.add(zoieSystem);
		  }
		  
		  SenseiIndexLoader loader = indexLoaderFactory.getIndexLoader(part, zoieSystem);
		  if(!indexLoaders.contains(loader))
		  {
		    loader.start();
		    indexLoaders.add(loader);
		  }
		  readerFactoryMap.put(part, zoieSystem);
		}
		
		SenseiSearchContext ctx = new SenseiSearchContext(builderFactoryMap, readerFactoryMap);
		SenseiNodeMessageHandler msgHandler = new SenseiNodeMessageHandler(ctx);
	
		// create the zookeeper cluster client
//		SenseiClusterClientImpl senseiClusterClient = new SenseiClusterClientImpl(clusterName, zookeeperURL, zookeeperTimeout, false);
		
		_node = new SenseiNode(clusterClient, _id, _port, msgHandler, _partitions);
		_node.startup(available);
		
		SenseiServerAdminMBean mbean = getAdminMBean();
		
		mbeanServer.registerMBean(new StandardMBean(mbean, SenseiServerAdminMBean.class),
                                  new ObjectName(clusterName, "name", "sensei-server"));
		
		Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run(){
				try {
					_node.shutdown();
				} catch (Exception e) {
					logger.error(e.getMessage(),e);
				}
                // shutdown the loaders
                for(SenseiIndexLoader loader : indexLoaders)
                {
                  try{
                    loader.shutdown();
                  }
                  catch(SenseiException se){
                	  logger.error(se.getMessage(),se);
                  }
                }
                // shutdown the zoieSystems
                for(ZoieSystem<BoboIndexReader,?> zoieSystem : zoieSystems)
                {
                  zoieSystem.shutdown();
                }
			}
		});
	}
	
	private SenseiServerAdminMBean getAdminMBean()
	{
	  return new SenseiServerAdminMBean()
      {
	    public int getId()
	    {
	      return _id;
	    }
	    public int getPort()
	    {
	      return _port;
	    }
	    public String getPartitions()
	    {
	      return _partitionString;
	    }
        public boolean isAvailable()
        {
          return _node.isAvailable();
        }
        public void setAvailable(boolean available)
        {
          _node.setAvailable(available);
        }
      };
	}
	
	public static void main(String[] args) throws Exception{
	  if (args.length<4){
	    System.out.println(help());
	    System.exit(1);
	  }
	  
	  int id = 0;
	  int port = 0;
	  int[] partitions = null;
	  String[] partString = null;
	        
	  File confDir = null;
	  
	  try{
	    id = Integer.parseInt(args[0]);
	    port = Integer.parseInt(args[1]);
	    partString = args[2].split(",");
	    confDir = new File(args[3]);
	    
	    partitions = new int[partString.length];
	    for (int i=0;i<partString.length;++i){
	      partitions[i] = Integer.parseInt(partString[i]);
	    }
	  }
	  catch(Exception e){
	    System.out.println(help());
	    System.exit(0);
	  }
	  boolean available = true;
	  for(int i = 4; i < args.length; i++)
	  {
	    if(args[i] != null)
	    {
	      if(AVAILABLE.equalsIgnoreCase(args[i]))
	      {
	        available = true;
	      }
	      if(UNAVAILABLE.equalsIgnoreCase(args[i]))
	      {
	        available = false;
	      }
	    }
	  }
	  SenseiServer server = new SenseiServer(id, port, partitions, confDir);
	  server.start(available);
	}
}
