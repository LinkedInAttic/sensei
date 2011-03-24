package com.sensei.search.nodes;

import java.io.File;
import java.io.FilenameFilter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieIndexReader;
import scala.actors.threadpool.Arrays;

import com.browseengine.bobo.api.BoboIndexReader;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.NetworkServer;
import com.linkedin.norbert.network.NetworkingException;
import com.sensei.conf.SenseiServerBuilder;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiResultBPO;
import com.sensei.search.svc.api.SenseiException;
import com.sensei.search.svc.impl.CoreSenseiServiceImpl;

public class SenseiServer {
  private static final Logger logger = Logger.getLogger(SenseiServer.class);
  
    private static final String AVAILABLE = "available";
    private static final String UNAVAILABLE = "unavailable";  
  
    private int _id;
    private int _port;
    private int[] _partitions;
    private NetworkServer _networkServer;
    private ClusterClient _clusterClient;
    private final MBeanServer mbeanServer = java.lang.management.ManagementFactory.getPlatformMBeanServer();
    private final SenseiCore _core;
    private final List<ObjectName> _registeredMBeans;
    protected volatile Node _serverNode;
    private final CoreSenseiServiceImpl _innerSvc;
    private final List<AbstractSenseiNodeMessageHandler> _externalMessageHandlerList;

    protected volatile boolean _available = false;
    
    public SenseiServer(int id, int port, int[] partitions,
            NetworkServer networkServer,
            ClusterClient clusterClient,
            SenseiZoieFactory<?,?> zoieSystemFactory,
            SenseiIndexLoaderFactory indexLoaderFactory,
            SenseiQueryBuilderFactory queryBuilderFactory,
            List<AbstractSenseiNodeMessageHandler> externalMessageHandlerList){
      this(id,port,partitions,null,networkServer,clusterClient,zoieSystemFactory,indexLoaderFactory,queryBuilderFactory,externalMessageHandlerList);
    }
    
    public SenseiServer(int id, int port, int[] partitions,
                        File extDir,
                        NetworkServer networkServer,
                        ClusterClient clusterClient,
                        SenseiZoieFactory<?,?> zoieSystemFactory,
                        SenseiIndexLoaderFactory indexLoaderFactory,
                        SenseiQueryBuilderFactory queryBuilderFactory,
                        List<AbstractSenseiNodeMessageHandler> externalMessageHandlerList)
    {
       this(id,port,partitions,networkServer,clusterClient,new SenseiCore(id, partitions, extDir,zoieSystemFactory, indexLoaderFactory, queryBuilderFactory),externalMessageHandlerList);
    }
    
    public SenseiServer(int id, int port, int[] partitions,
            NetworkServer networkServer,
            ClusterClient clusterClient,
            SenseiCore senseiCore,
            List<AbstractSenseiNodeMessageHandler> externalMessageHandlerList)
   {
    	_registeredMBeans = new LinkedList<ObjectName>();
        _core = senseiCore;
        _id = id;
        _port = port;
        _partitions = partitions;
       
        _networkServer = networkServer;
        _clusterClient = clusterClient;
        
        _innerSvc = new CoreSenseiServiceImpl(senseiCore);
        _externalMessageHandlerList = externalMessageHandlerList;
   }
    
  private static String help(){
    StringBuffer buffer = new StringBuffer();
    buffer.append("Usage: <conf.dir> [availability]\n");
    buffer.append("====================================\n");
    buffer.append("conf.dir - server configuration directory, required\n");
    buffer.append("availability - \"available\" or \"unavailable\", optional default is \"available\"\n");
    buffer.append("====================================\n");
    return buffer.toString();
  }
  
  /*
  public Collection<Zoie<BoboIndexReader,?,?>> getZoieSystems(){
    return _core.zoieSystems;
  }
  */
  
  
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
    	logger.info("shutting down node...");
        try
        {
          _core.shutdown();
          _clusterClient.removeNode(_id);
          _serverNode = null;
        } catch (Exception e)
        {
          logger.warn(e.getMessage());
        } finally
        {
          if (_networkServer != null)
          {
        	  _networkServer.shutdown();
          }
        }
    } catch (Exception e) {
      logger.error(e.getMessage(),e);
    }
  }
  
  public void start(boolean available) throws Exception{        
        _core.start();
//        ClusterClient clusterClient = ClusterClientFactory.newInstance().newZookeeperClient();
      String clusterName = _clusterClient.getServiceName();
   
      logger.info("Cluster Name: " + clusterName);
      logger.info("Cluster info: " + _clusterClient.toString());
    
    // create the zookeeper cluster client
//    SenseiClusterClientImpl senseiClusterClient = new SenseiClusterClientImpl(clusterName, zookeeperURL, zookeeperTimeout, false);
    SenseiNodeMessageHandler senseiMsgHandler =  new SenseiNodeMessageHandler(_innerSvc);
    _networkServer.registerHandler(senseiMsgHandler.getRequestMessage(),senseiMsgHandler.getResponseMessage(),senseiMsgHandler);
    if (_externalMessageHandlerList!=null){
    	for (AbstractSenseiNodeMessageHandler handler : _externalMessageHandlerList){
    		_networkServer.registerHandler(handler.getRequestMessage(),handler.getResponseMessage(), handler);
    	}
    }
    HashSet<Integer> partition = new HashSet<Integer>();
    for (int partId : _partitions){
    	partition.add(partId);
    }
    
    boolean nodeExists = false;
    try
    {
      logger.info("waiting to connect to cluster...");
      _clusterClient.awaitConnectionUninterruptibly();
      _serverNode = _clusterClient.getNodeWithId(_id);
      nodeExists = (_serverNode != null);
      if (!nodeExists)
      {
        String ipAddr = (new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), _port)).toString().replaceAll("/", "");

        logger.info("Node id : " + _id + " IP address : " + ipAddr);

        _serverNode = _clusterClient.addNode(_id, ipAddr, partition);

        logger.info("added node id: " + _id);
      } else
      {
        // node exists

      }
    } catch (Exception e)
    {
      logger.error(e.getMessage(), e);
      throw e;
    }

    try
    {
      logger.info("binding server ...");
      _networkServer.bind(_id, available);

      // exponential backoff
      Thread.sleep(1000);

      _available = available;
      logger.info("started [markAvailable=" + available + "] ...");
      if (nodeExists)
      {
        logger.warn("existing node found, will try to overwrite.");
        try
        {
          // remove node above
          _clusterClient.removeNode(_id);
          _serverNode = null;
        } catch (Exception e)
        {
          logger.error("problem removing old node: " + e.getMessage(), e);
        }
        String ipAddr = (new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), _port)).toString().replaceAll("/", "");
        _serverNode = _clusterClient.addNode(_id, ipAddr, partition);
        Thread.sleep(1000);

        logger.info("added node id: " + _id);
      }
    } catch (NetworkingException e)
    {
      logger.error(e.getMessage(), e);

      try
      {
        if (!nodeExists)
        {
          _clusterClient.removeNode(_id);
          _serverNode = null;
        }
      } catch (Exception ex)
      {
        logger.warn(ex.getMessage());
      } finally
      {
        try
        {
          _networkServer.shutdown();
          _networkServer = null;

        } finally
        {
          _clusterClient.shutdown();
          _clusterClient = null;
        }
      }
      throw e;
    }

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
	  return new SenseiServerAdminMBean(){
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
        StringBuffer sb = new StringBuffer();
        if(_partitions.length > 0) sb.append(String.valueOf(_partitions[0]));
         for(int i = 1; i < _partitions.length; i++)
         {
             sb.append(',');
             sb.append(String.valueOf(_partitions[i]));
         }
        return sb.toString();
      }
        public boolean isAvailable()
        {
          return SenseiServer.this.isAvailable();
        }
        public void setAvailable(boolean available)
        {
          SenseiServer.this.setAvailable(available);
        }
      };
  }
	
  public void setAvailable(boolean available){
    if (available)
    {
      logger.info("making available node " + _id + " @port:" + _port + " for partitions: " + Arrays.toString(_partitions));
      _networkServer.markAvailable();
      try
      {
        Thread.sleep(1000);
      } catch (InterruptedException e)
      {
      }
    } else
    {
      logger.info("making unavailable node " + _id + " @port:" + _port + " for partitions: " + Arrays.toString(_partitions));
      _networkServer.markUnavailable();
    }
    _available = available;
  }

  public boolean isAvailable()
  {
    if (_serverNode != null && _serverNode.isAvailable() == _available)
      return _available;

    try
    {
      Thread.sleep(1000);
      _serverNode = _clusterClient.getNodeWithId(_id);
      if (_serverNode != null && _serverNode.isAvailable() == _available)
        return _available;
    } catch (Exception e)
    {
      logger.error(e.getMessage(), e);
    }
    _available = (_serverNode != null ? _serverNode.isAvailable() : false);

    return _available;
  }

  public  static void main(String[] args) throws Exception {
    if (args.length<1){
      System.out.println(help());
      System.exit(1);
    }
    
    File confDir = null;
    
    try {
      confDir = new File(args[0]);
    }
    catch(Exception e) {
      System.out.println(help());
      System.exit(1);
    }

    boolean available = true;
    for(int i = 1; i < args.length; i++)
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
    
    SenseiServerBuilder senseiServerBuilder = new SenseiServerBuilder(confDir);

    final SenseiServer server = senseiServerBuilder.buildServer();

    Runtime.getRuntime().addShutdownHook(new Thread(){
      public void run(){
        server.shutdown();
      }
    });
    
    server.start(available);
  }
  
}
