package com.sensei.search.nodes;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.management.InstanceAlreadyExistsException;
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
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient;
import com.linkedin.norbert.javacompat.network.NetworkServer;
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
    private NetworkServer _networkServer;
    private ClusterClient _clusterClient;
    private SenseiZoieSystemFactory<?,?> _zoieSystemFactory;
    private SenseiIndexLoaderFactory _indexLoaderFactory;
    private SenseiQueryBuilderFactory _queryBuilderFactory;
    private SenseiNode _node;
    private final HashSet<ZoieSystem<BoboIndexReader,?,?>> zoieSystems = new HashSet<ZoieSystem<BoboIndexReader,?,?>>();
    private final HashSet<SenseiIndexLoader> indexLoaders = new HashSet<SenseiIndexLoader>();
    private final MBeanServer mbeanServer = java.lang.management.ManagementFactory.getPlatformMBeanServer();
    
    private final List<ObjectName> _registeredMBeans;
	
    
    public SenseiServer(int id, int port, int[] partitions,
            NetworkServer networkServer,
            ClusterClient clusterClient,
            SenseiZoieSystemFactory<?,?> zoieSystemFactory,
            SenseiIndexLoaderFactory indexLoaderFactory,
            SenseiQueryBuilderFactory queryBuilderFactory){
    	this(id,port,partitions,null,networkServer,clusterClient,zoieSystemFactory,indexLoaderFactory,queryBuilderFactory);
    }
    
    public SenseiServer(int id, int port, int[] partitions,
                        File extDir,
                        NetworkServer networkServer,
                        ClusterClient clusterClient,
                        SenseiZoieSystemFactory<?,?> zoieSystemFactory,
                        SenseiIndexLoaderFactory indexLoaderFactory,
                        SenseiQueryBuilderFactory queryBuilderFactory)
    {
      _registeredMBeans = new LinkedList<ObjectName>();
      if (extDir!=null){
        loadJars(extDir);
      }
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
      _networkServer = networkServer;
      _clusterClient = clusterClient;
      _zoieSystemFactory = zoieSystemFactory;
      _indexLoaderFactory = indexLoaderFactory;
      _queryBuilderFactory = queryBuilderFactory;
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
	
	public Collection<ZoieSystem<BoboIndexReader,?,?>> getZoieSystems(){
		return zoieSystems;
	}
	
	private static void loadJars(File extDir)
	{
	  File[] jarfiles = extDir.listFiles(new FilenameFilter(){
        public boolean accept(File dir, String name) {
            return name.endsWith(".jar");
        }
	  });
      
	  if (jarfiles!=null && jarfiles.length > 0){
		try{
	      URL[] jarURLs = new URL[jarfiles.length];
          ClassLoader parentLoader = Thread.currentThread().getContextClassLoader();
          for (int i=0;i<jarfiles.length;++i){
            jarURLs[i] = new URL("jar:file://" + jarfiles[i].getAbsolutePath() + "!/");  
          }
          URLClassLoader classloader = new URLClassLoader(jarURLs,parentLoader);
          Thread.currentThread().setContextClassLoader(classloader);
		}
		catch(MalformedURLException e){
			logger.error("problem loading extension: "+e.getMessage(),e);
		}
	  }
	}
	
	public void shutdown(){
		logger.info("unregistering mbeans...");
		try{
			if (_registeredMBeans.size()>0){
				for (ObjectName name : _registeredMBeans){
				  mbeanServer.unregisterMBean(name);
				}
				_registeredMBeans.clear();
			}
		}
		catch(Exception e){
			logger.error(e.getMessage(),e);
		}
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
        indexLoaders.clear();
        // shutdown the zoieSystems
        for(ZoieSystem<BoboIndexReader,?,?> zoieSystem : zoieSystems)
        {
          zoieSystem.shutdown();
        }
        zoieSystems.clear();
	}
	
	public void start(boolean available) throws Exception
	{        
        Map<Integer,SenseiQueryBuilderFactory> builderFactoryMap = new HashMap<Integer, SenseiQueryBuilderFactory>();
        
		Map<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>> readerFactoryMap = 
				new HashMap<Integer,IndexReaderFactory<ZoieIndexReader<BoboIndexReader>>>();
        
//        ClusterClient clusterClient = ClusterClientFactory.newInstance().newZookeeperClient();
        String clusterName = _clusterClient.getServiceName();
        
        logger.info("ClusterName: " + clusterName);
        logger.info("Cluster info: " + _clusterClient.toString());

		for (int part : _partitions){
		  //in simple case query builder is the same for each partition
		  builderFactoryMap.put(part, _queryBuilderFactory);
			
		  ZoieSystem<BoboIndexReader,?,?> zoieSystem = _zoieSystemFactory.getZoieSystem(_id,part);
		  
		  // register ZoieSystemAdminMBean

		  ObjectName name = new ObjectName(clusterName, "name", "zoie-system-" + _id+"-"+part);
		  try{
		    mbeanServer.registerMBean(new StandardMBean(zoieSystem.getAdminMBean(), ZoieSystemAdminMBean.class),name);
		    _registeredMBeans.add(name);
		  }
		  catch(Exception e){
			  logger.error(e.getMessage(),e);
			  if (e instanceof InstanceAlreadyExistsException){
				  _registeredMBeans.add(name);
			  }
		  }
		  
		  // register ZoieIndexingStatusAdminMBean
		  name = new ObjectName(clusterName, "name", "zoie-indexing-status-" + _id+"-"+ part);
		  try{
		    mbeanServer.registerMBean(new StandardMBean(new ZoieIndexingStatusAdmin(zoieSystem), ZoieIndexingStatusAdminMBean.class),name);
		    _registeredMBeans.add(name);
		  }
		  catch(Exception e){
			  logger.error(e.getMessage(),e);
			  if (e instanceof InstanceAlreadyExistsException){
				  _registeredMBeans.add(name);
			  }
		  }
	          	  
		  
		  if(!zoieSystems.contains(zoieSystem))
		  {
		    zoieSystem.start();
		    zoieSystems.add(zoieSystem);
		  }
		  
		  SenseiIndexLoader loader = _indexLoaderFactory.getIndexLoader(part, zoieSystem);
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
		
		_node = new SenseiNode(_networkServer, _clusterClient, _id, _port, msgHandler, _partitions);
		_node.startup(available);

		ObjectName name = new ObjectName(clusterName, "name", "sensei-server-"+_id);
		try{
		  SenseiServerAdminMBean mbean = getAdminMBean();
		  mbeanServer.registerMBean(new StandardMBean(mbean, SenseiServerAdminMBean.class),name);
		  _registeredMBeans.add(name);
		}
		catch(Exception e){
			logger.error(e.getMessage(),e);
			if (e instanceof InstanceAlreadyExistsException){
			  _registeredMBeans.add(name);
		    }
		}
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
	  
	  File confFile = new File(confDir,DEFAULT_CONF_FILE);
	  File extDir = new File(confDir,"ext");
	  
	  ApplicationContext springCtx = new FileSystemXmlApplicationContext("file:"+confFile.getAbsolutePath());
	  
	  NetworkServer networkServer = (NetworkServer)springCtx.getBean("network-server");
      ClusterClient clusterClient = (ZooKeeperClusterClient)springCtx.getBean("cluster-client");
      SenseiZoieSystemFactory<?,?> zoieSystemFactory = (SenseiZoieSystemFactory<?,?>)springCtx.getBean("zoie-system-factory");
      SenseiIndexLoaderFactory<?,?> indexLoaderFactory = (SenseiIndexLoaderFactory)springCtx.getBean("index-loader-factory");
      SenseiQueryBuilderFactory queryBuilderFactory = (SenseiQueryBuilderFactory)springCtx.getBean("query-builder-factory");
      
	  final SenseiServer server = new SenseiServer(id, port, partitions,
	                                         extDir,
	                                         networkServer,
	                                         clusterClient,
	                                         zoieSystemFactory,
	                                         indexLoaderFactory,
	                                         queryBuilderFactory);
	  
	  Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run(){
				server.shutdown();
			}
		});
	  
	  server.start(available);
	}
}
