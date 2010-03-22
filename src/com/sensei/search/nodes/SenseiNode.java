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
	private ServerBootstrap _bootstrap;
	private NetworkServer _server;
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

	public void startup() throws Exception{
		ServerConfig serverConfig = new ServerConfig();
		serverConfig.setClusterName(_clusterName);
		serverConfig.setZooKeeperUrls(_zookeeperURL);
		serverConfig.setMessageHandlers(_msgHandlers);
		
		serverConfig.setNodeId(_id);
		
		ServerBootstrapFactory bootstrapFactory = getServerBootstrapFactory();
		
		_bootstrap = bootstrapFactory.getServerBootstrap(serverConfig);

		Cluster cluster = _bootstrap.getCluster();

		boolean nodeExists = false;
		try{
		  logger.info("waiting to connect to cluster...");
		  cluster.awaitConnection();
		  Node node = cluster.getNodeWithId(_id);
		  nodeExists = (node!=null); 
		  if (!nodeExists){
			  node = cluster.addNode(_id, new InetSocketAddress(InetAddress.getLocalHost(),_port),_partitions);
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
	    	_server.bind();
			logger.info("started...");
			if (nodeExists){
			  logger.warn("existing node found, will try to overwrite.");
			  try{
			    cluster.removeNode(_id);
			  }
			  catch(Exception e){
				logger.error("problem removing old node: "+e.getMessage(),e);
			  }
			  cluster.addNode(_id, new InetSocketAddress(InetAddress.getLocalHost(),_port),_partitions);
			  Thread.sleep(1000);

			  logger.info("added node id: "+_id);
		    }
	    } catch (NetworkingException e) {
	    	logger.error(e.getMessage(),e);
	    	 
			try
			{
			  if (!nodeExists){
			    cluster.removeNode(_id);
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
	
	public void shutdown() throws Exception{
		logger.info("shutting down...");
		try
		{
		  Cluster cluster = _bootstrap.getCluster();
		  cluster.removeNode(_id);
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
