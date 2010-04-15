package com.sensei.search.nodes;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.log4j.Logger;

import com.linkedin.norbert.cluster.Node;
import com.linkedin.norbert.cluster.javaapi.ClusterClient;
import com.linkedin.norbert.network.NetworkingException;
import com.linkedin.norbert.network.javaapi.MessageHandler;
import com.linkedin.norbert.network.javaapi.NetworkServer;
import com.sensei.search.cluster.client.SenseiClusterClientImpl;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiResultBPO;
import com.sensei.search.server.SenseiNetworkServer;

public class SenseiNode{
	private static Logger logger = Logger.getLogger(SenseiNode.class);
	
	private final String _zookeeperURL;
	private final int _id;
	private final MessageHandler _msgHandler;
	private final int[] _partitions;
	private final String _clusterName;
	private ClusterClient _cluster;
	private NetworkServer _server;
	private volatile Node _node;
	private volatile boolean _available = false;
	private final int _port;
    private int _zooKeeperSessionTimeoutMillis; 
    private SenseiClusterClientImpl _senseiClusterClient;
	
	public SenseiNode(String clusterName,int id,int port,MessageHandler msgHandler,String zookeeperURL,int[] partitions,
	                  int zooKeeperSessionTimeoutMillis){
		_id = id;
		_port = port;
		_msgHandler = msgHandler;
		_zookeeperURL = zookeeperURL;
		_clusterName = clusterName;
		_zooKeeperSessionTimeoutMillis = zooKeeperSessionTimeoutMillis;
		_partitions = partitions;
		_senseiClusterClient = null;
	}
	
	public ClusterClient getClusterClient() 
	{
	  if(_senseiClusterClient == null)
	    _senseiClusterClient = new SenseiClusterClientImpl(_clusterName, true);

	  _cluster = _senseiClusterClient.getClusterClient();
	  return _cluster;
	}

	public void setClusterClient(SenseiClusterClientImpl senseiClusterClient)
	{
	  _senseiClusterClient = senseiClusterClient;
	}
	
	public void startup(boolean markAvailable) throws Exception{
      _cluster = this.getClusterClient();
      
	  _server = new SenseiNetworkServer(_cluster);
	  _server.registerHandler(SenseiRequestBPO.Request.getDefaultInstance(), SenseiResultBPO.Result.getDefaultInstance(), _msgHandler);

	  boolean nodeExists = false;
	  try{
	    logger.info("waiting to connect to cluster...");
	    _cluster.awaitConnectionUninterruptibly();
	    _node = _cluster.getNodeWithId(_id);
	    nodeExists = (_node!=null); 
	    if (!nodeExists){
	      _node = _cluster.addNode(_id, (new InetSocketAddress(InetAddress.getLocalHost(),_port)).toString(), _partitions);
	      Thread.sleep(1000);

	      logger.info("added node id: "+_id);
	    }
	  }
	  catch(Exception e){
	    logger.error(e.getMessage(),e);
	    throw e;
	  }

	  //		_server = _bootstrap.getNetworkServer();
	  //	    
	  try {
	    logger.info("binding server ...");
	    _server.bind(_id);
	    if(markAvailable)
	      _cluster.markNodeAvailable(_id);
	    else
	      _cluster.markNodeUnavailable(_id);
	    _available = markAvailable;
	    logger.info("started [markAvailable=" + markAvailable + "] ...");
	    if (nodeExists){
	      logger.warn("existing node found, will try to overwrite.");
	      try{
	        _cluster.removeNode(_id);
	        _node = null;
	      }
	      catch(Exception e){
	        logger.error("problem removing old node: "+e.getMessage(),e);
	      }
	      _node = _cluster.addNode(_id, (new InetSocketAddress(InetAddress.getLocalHost(),_port)).toString(),_partitions);
	      Thread.sleep(1000);

	      logger.info("added node id: "+_id);
	    }
	  } catch (NetworkingException e) {
	    logger.error(e.getMessage(),e);

	    try
	    {
	      if (!nodeExists){
	        _cluster.removeNode(_id);
	        _node = null;
	      }
	    }
	    catch(Exception ex){
	      logger.warn(ex.getMessage());
	    }
	    finally{
	      try{
	        _server.shutdown();
	        _server = null;

	      }
	      finally{
	        _cluster.shutdown();
	        _cluster = null;
	      }
	    }
	    throw e;
	  }
	}

	public void setAvailable(boolean available)
	{
	  if(available)
	  {
	    logger.info("making node available");
	    _server.markAvailable();
	    try
	    {
	      Thread.sleep(1000);
	    }
	    catch (InterruptedException e)
	    {
	    }
	  }
	  _available = available;
	}

	public boolean isAvailable()
	{
	  if(_node != null && _node.isAvailable() == _available) return _available;

	  try
	  {
	    Thread.sleep(1000);
	    _node = _cluster.getNodeWithId(_id);
	    if(_node != null && _node.isAvailable() == _available) return _available;
	  }
	  catch (Exception e)
	  {
	    logger.error(e.getMessage(), e);
	  }
	  _available = (_node != null ? _node.available() : false);

	  return _available;
	}

	public void shutdown() throws Exception{
	  logger.info("shutting down...");
	  try
	  {
	    //		  Cluster cluster = _bootstrap.getCluster();
	    _cluster.removeNode(_id);
	    _node = null;
	  }
	  catch(Exception e){
	    logger.warn(e.getMessage());
	  }
	  finally{
	    try{
	      if (_server!=null){
	        _server.shutdown();
	      }
	    }
	    finally{
	      if (_cluster!=null){
	        _cluster.shutdown();
	      }
	    }
	  }
	}
}
