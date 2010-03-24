package com.sensei.search.nodes;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.log4j.Logger;

import com.linkedin.norbert.cluster.Node;
import com.linkedin.norbert.cluster.javaapi.Cluster;
import com.linkedin.norbert.network.NetworkingException;
import com.linkedin.norbert.network.javaapi.MessageHandler;
import com.linkedin.norbert.network.javaapi.NetworkServer;
import com.linkedin.norbert.network.javaapi.ServerBootstrap;
import com.linkedin.norbert.network.javaapi.ServerConfig;

public class SenseiNode{
	private static Logger logger = Logger.getLogger(SenseiNode.class);
	
	private final String _zookeeperURL;
	private final int _id;
	private final MessageHandler[] _msgHandlers;
	private final int[] _partitions;
	private final String _clusterName;
	private Cluster _cluster;
	private ServerBootstrap _bootstrap;
	private NetworkServer _server;
	private volatile Node _node;
	private volatile boolean _available = false;
	private final int _port;
	private ServerBootstrapFactory _bootstrapFactory;
	 
	
	public SenseiNode(String clusterName,int id,int port,MessageHandler[] msgHandlers,String zookeeperURL,int[] partitions){
		_id = id;
		_port = port;
		_msgHandlers = msgHandlers;
		_zookeeperURL = zookeeperURL;
		_clusterName = clusterName;
		_partitions = partitions;
		_bootstrapFactory = null;
	}
	
	public void setServerBootstrapFactory(ServerBootstrapFactory bootstrapFactory){
		_bootstrapFactory = bootstrapFactory;
	}
	
	public ServerBootstrapFactory getServerBootstrapFactory(){
		return _bootstrapFactory == null ? new ServerBootstrapFactory.DefaultServerBootstrapFactory() : _bootstrapFactory;
	}

	public void startup(boolean available) throws Exception{
		ServerConfig serverConfig = new ServerConfig();
		serverConfig.setClusterName(_clusterName);
		serverConfig.setZooKeeperUrls(_zookeeperURL);
		serverConfig.setMessageHandlers(_msgHandlers);
		
		serverConfig.setNodeId(_id);
		
		ServerBootstrapFactory bootstrapFactory = getServerBootstrapFactory();
		
		_bootstrap = bootstrapFactory.getServerBootstrap(serverConfig);
		_cluster = _bootstrap.getCluster();
		
		boolean nodeExists = false;
		try{
		  logger.info("waiting to connect to cluster...");
		  _cluster.awaitConnection();
		  _node = _cluster.getNodeWithId(_id);
		  nodeExists = (_node!=null); 
		  if (!nodeExists){
			  _node = _cluster.addNode(_id, new InetSocketAddress(InetAddress.getLocalHost(),_port),_partitions);
			  Thread.sleep(1000);

			  logger.info("added node id: "+_id);
		  }
        }
        catch(Exception e){
          logger.error(e.getMessage(),e);
          throw e;
        }
	        
		_server = _bootstrap.getNetworkServer();
	    
	    try {
			logger.info("binding server ...");
	    	_server.bind(available);
	        _available = available;
			logger.info("started [available=" + available + "] ...");
			if (nodeExists){
			  logger.warn("existing node found, will try to overwrite.");
			  try{
			    _cluster.removeNode(_id);
			    _node = null;
			  }
			  catch(Exception e){
				logger.error("problem removing old node: "+e.getMessage(),e);
			  }
			  _node = _cluster.addNode(_id, new InetSocketAddress(InetAddress.getLocalHost(),_port),_partitions);
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
				  _bootstrap.shutdown();
				  _bootstrap = null;
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
		  Cluster cluster = _bootstrap.getCluster();
		  cluster.removeNode(_id);
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
			  if (_bootstrap!=null){
			    _bootstrap.shutdown();
			  }
			}
		}
	}
}
