package com.sensei.search.nodes;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import scala.actors.threadpool.Arrays;

import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.NetworkServer;
import com.linkedin.norbert.network.NetworkingException;
import com.sensei.search.req.protobuf.SenseiSysRequestBPO;
import com.sensei.search.req.protobuf.SenseiSysResultBPO;

public abstract class AbstractSenseiNode
{
  private static Logger logger = Logger.getLogger(AbstractSenseiNode.class);

  protected final int _id;
  protected final SenseiSearchContext _context;
  protected final Set<Integer> _partitions;
  protected ClusterClient _cluster;
  protected NetworkServer _server;
  protected volatile Node _node;
  protected volatile boolean _available = false;
  protected final int _port;

  public AbstractSenseiNode(NetworkServer server, ClusterClient client, int id, int port, SenseiSearchContext context, int[] partitions)
  {
    _id = id;
    _port = port;
    _context = context;
    _partitions = new HashSet<Integer>();
    for (int partition : partitions)
    {
      _partitions.add(partition);
    }
    _cluster = client;
    if (_cluster == null)
      throw new IllegalArgumentException("Valid cluster client should be specified ");
    _server = server;
    if (_server == null)
      throw new IllegalArgumentException("Valid network server should be specified ");
  }

  public void setClusterClient(ClusterClient senseiClusterClient)
  {
    _cluster = senseiClusterClient;
  }
  
  public abstract void initMessageHandlers();

  public void startup(boolean markAvailable) throws Exception
  {
    initMessageHandlers();
    SenseiNodeSysMessageHandler sysMsgHandler = new SenseiNodeSysMessageHandler(_id, _context);
    _server.registerHandler(SenseiSysRequestBPO.SysRequest.getDefaultInstance(), SenseiSysResultBPO.SysResult.getDefaultInstance(), sysMsgHandler);

    boolean nodeExists = false;
    try
    {
      logger.info("waiting to connect to cluster...");
      _cluster.awaitConnectionUninterruptibly();
      _node = _cluster.getNodeWithId(_id);
      nodeExists = (_node != null);
      if (!nodeExists)
      {
        String ipAddr = (new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), _port)).toString().replaceAll("/", "");

        logger.info("Node id : " + _id + " IP address : " + ipAddr);

        _node = _cluster.addNode(_id, ipAddr, _partitions);

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
      _server.bind(_id, markAvailable);

      // exponential backoff
      Thread.sleep(1000);

      _available = markAvailable;
      logger.info("started [markAvailable=" + markAvailable + "] ...");
      if (nodeExists)
      {
        logger.warn("existing node found, will try to overwrite.");
        try
        {
          // remove node above
          _cluster.removeNode(_id);
          _node = null;
        } catch (Exception e)
        {
          logger.error("problem removing old node: " + e.getMessage(), e);
        }
        String ipAddr = (new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), _port)).toString().replaceAll("/", "");
        _node = _cluster.addNode(_id, ipAddr, _partitions);
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
          _cluster.removeNode(_id);
          _node = null;
        }
      } catch (Exception ex)
      {
        logger.warn(ex.getMessage());
      } finally
      {
        try
        {
          _server.shutdown();
          _server = null;

        } finally
        {
          _cluster.shutdown();
          _cluster = null;
        }
      }
      throw e;
    }
  }

  public void setAvailable(boolean available)
  {
    if (available)
    {
      logger.info("making available node " + _id + " @port:" + _port + " for partitions: " + Arrays.toString(_partitions.toArray(new Integer[0])));
      _server.markAvailable();
      try
      {
        Thread.sleep(1000);
      } catch (InterruptedException e)
      {
      }
    } else
    {
      logger.info("making unavailable node " + _id + " @port:" + _port + " for partitions: " + Arrays.toString(_partitions.toArray(new Integer[0])));
      _server.markUnavailable();
    }
    _available = available;
  }

  public boolean isAvailable()
  {
    if (_node != null && _node.isAvailable() == _available)
      return _available;

    try
    {
      Thread.sleep(1000);
      _node = _cluster.getNodeWithId(_id);
      if (_node != null && _node.isAvailable() == _available)
        return _available;
    } catch (Exception e)
    {
      logger.error(e.getMessage(), e);
    }
    _available = (_node != null ? _node.isAvailable() : false);

    return _available;
  }

  public void shutdown() throws Exception
  {
    logger.info("shutting down node...");
    try
    {
      _cluster.removeNode(_id);
      _node = null;
    } catch (Exception e)
    {
      logger.warn(e.getMessage());
    } finally
    {
      if (_server != null)
      {
        _server.shutdown();
      }
    }
  }
}
