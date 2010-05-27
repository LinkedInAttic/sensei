package com.sensei.search.nodes;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.norbert.cluster.javaapi.ClusterClient;
import com.linkedin.norbert.cluster.javaapi.Node;
import com.linkedin.norbert.network.NetworkingException;
import com.linkedin.norbert.network.javaapi.MessageHandler;
import com.linkedin.norbert.network.javaapi.NetworkServer;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiResultBPO;

public class SenseiNode{
	private static Logger logger = Logger.getLogger(SenseiNode.class);
	
	private final int _id;
	private final MessageHandler _msgHandler;
	private final Set<Integer> _partitions;
	private ClusterClient _cluster;
	private NetworkServer _server;
	private volatile Node _node;
	private volatile boolean _available = false;
	private final int _port;
	
	public SenseiNode(NetworkServer server, ClusterClient client, int id, int port, MessageHandler msgHandler, int[] partitions){
		_id = id;
		_port = port;
		_msgHandler = msgHandler;
        _partitions = new HashSet<Integer>();
		for(int partition : partitions) {
		  _partitions.add(partition);
		}
        _cluster = client;
        if(_cluster == null)
          throw new IllegalArgumentException("Valid cluster client should be specified ");
        _server = server;
        if(_server == null)
          throw new IllegalArgumentException("Valid network server should be specified ");
	}
	
	public void setClusterClient(ClusterClient senseiClusterClient) {
	  _cluster = senseiClusterClient;
	}
	
	public void startup(boolean markAvailable) throws Exception{      
	  
	  _server.registerHandler(SenseiRequestBPO.Request.getDefaultInstance(), SenseiResultBPO.Result.getDefaultInstance(), _msgHandler);

	  boolean nodeExists = false;
	  try{
	    logger.info("waiting to connect to cluster...");
	    _cluster.awaitConnectionUninterruptibly();
	    _node = _cluster.getNodeWithId(_id);
	    nodeExists = (_node!=null); 
	    if (!nodeExists){
	      String ipAddr = (new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), _port)).toString().replaceAll("/", "");
	      
	      System.out.println("Node id : " + _id + " IP address : " + ipAddr);
	      
	      _node = _cluster.addNode(_id, ipAddr, _partitions);

	      logger.info("added node id: "+_id);
	    }else {
	      // node exists 
	      
	    }
	  }
	  catch(Exception e){
	    logger.error(e.getMessage(),e);
	    throw e;
	  }

	  try {
	    logger.info("binding server ...");
	    _server.bind(_id, markAvailable);

	    // exponential backoff
	    Thread.sleep(1000);

	    _available = markAvailable;
	    logger.info("started [markAvailable=" + markAvailable + "] ...");
	    if (nodeExists){
	      logger.warn("existing node found, will try to overwrite.");
	      try{
	        // remove node above 
	        _cluster.removeNode(_id);
	        _node = null;
	      }
	      catch(Exception e){
	        logger.error("problem removing old node: "+e.getMessage(),e);
	      }
          String ipAddr = (new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), _port)).toString().replaceAll("/", "");
          _node = _cluster.addNode(_id, ipAddr, _partitions);
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
	  _available = (_node != null ? _node.isAvailable() : false);

	  return _available;
	}

	public void shutdown() throws Exception{
	  logger.info("shutting down...");
	  try
	  {
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
