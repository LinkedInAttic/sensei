package com.sensei.search.nodes;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Comparator;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import com.linkedin.norbert.javacompat.network.PartitionedNetworkClient;
import com.sensei.conf.SenseiSchema;
import com.sensei.search.cluster.routing.UniformPartitionedLoadBalancer;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.SenseiSystemInfo;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiRequestBPOConverter;
import com.sensei.search.req.protobuf.SenseiResultBPO;
import com.sensei.search.req.protobuf.SenseiSysRequestBPO;
import com.sensei.search.req.protobuf.SenseiSysRequestBPOConverter;
import com.sensei.search.req.protobuf.SenseiSysResultBPO;
import com.sensei.search.req.protobuf.SenseiRequestBPO.Request;
import com.sensei.search.req.protobuf.SenseiResultBPO.Result;
import com.sensei.search.svc.api.SenseiException;

/**
 * This SenseiBroker routes search(browse) request using the routers created by
 * the supplied router factory. It uses Norbert's scatter-gather handling
 * mechanism to handle distributed search, which does not support request based
 * context sensitive routing.
 */
public class SenseiBroker extends AbstractSenseiBroker<SenseiRequest, SenseiResult, SenseiRequestBPO.Request, SenseiResultBPO.Result>
{
  private final static Logger logger = Logger.getLogger(SenseiBroker.class);
  private final AbstractSenseiScatterGatherHandler<SenseiRequest, SenseiResult, SenseiRequestBPO.Request, SenseiResultBPO.Result> _scatterGatherHandler;
  private final SenseiSysScatterGatherHandler _sysScatterGatherHandler;

  public SenseiBroker(PartitionedNetworkClient<Integer> networkClient, ClusterClient clusterClient, SenseiRequestScatterRewriter reqRewriter,
      PartitionedLoadBalancerFactory<Integer> routerFactory, Comparator<String> versionComparator,
      SenseiSchema senseiSchema) throws NorbertException
  {
    super(networkClient, clusterClient, SenseiRequestBPO.Request.getDefaultInstance(), SenseiResultBPO.Result.getDefaultInstance(),routerFactory);
    _sysScatterGatherHandler = new SenseiSysScatterGatherHandler(versionComparator);
    _networkClient.registerRequest(SenseiSysRequestBPO.SysRequest.getDefaultInstance(), SenseiSysResultBPO.SysResult.getDefaultInstance());
    _scatterGatherHandler = new SenseiScatterGatherHandler(senseiSchema, reqRewriter);
    logger.info("created broker instance " + networkClient + " " + clusterClient + " " + routerFactory + " " + reqRewriter);
  }

  @Override
  public SenseiResult getEmptyResultInstance()
  {
    return new SenseiResult();
  }

  protected SenseiResult doBrowse(PartitionedNetworkClient<Integer> networkClient, SenseiRequest req, IntSet partitions) throws Exception
  {
    if (partitions != null && (partitions.size()) > 0)
    {
      SenseiRequestBPO.Request msg = requestToMessage(req);
      Set<Integer> partToSend = req.getPartitions();
      if (partToSend == null)
      {
        partToSend = partitions;
      }
      SenseiResult res;
      if (partToSend.size() > 0)
      {
        res = networkClient.sendMessage(partitions, msg, _scatterGatherHandler);
      } else
      {
        res = getEmptyResultInstance();
      }
      return res;
    } else
    {
      logger.warn("no server exist to handle request.");
      return getEmptyResultInstance();
    }
  }

  @Override
  public SenseiResult messageToResult(Result message)
  {
    return SenseiRequestBPOConverter.convert(message);
  }

  @Override
  public Request requestToMessage(SenseiRequest request)
  {
    return SenseiRequestBPOConverter.convert(request);
  }

  public void setTimeoutMillis(long timeoutMillis)
  {
    _scatterGatherHandler.setTimeoutMillis(timeoutMillis);
  }

  public long getTimeoutMillis()
  {
    return _scatterGatherHandler.getTimeoutMillis();
  }

  public void handleClusterConnected(Set<Node> nodes)
  {
    UniformPartitionedLoadBalancer router = (UniformPartitionedLoadBalancer) _routerFactory.newLoadBalancer(nodes);
    _partitions = router.getPartitions();
    logger.info("handleClusterConnected(): Received the list of nodes from norbert " + nodes.toString());
    logger.info("handleClusterConnected(): Received the list of partitions from router " + _partitions.toString());
  }

  public void handleClusterDisconnected()
  {
    logger.info("handleClusterDisconnected() called");
    _partitions = new IntOpenHashSet();
  }

  public void handleClusterNodesChanged(Set<Node> nodes)
  {
    UniformPartitionedLoadBalancer router = (UniformPartitionedLoadBalancer) _routerFactory.newLoadBalancer(nodes);
    _partitions = router.getPartitions();
    logger.info("handleClusterNodesChanged(): Received the list of nodes from norbert " + nodes.toString());
    logger.info("handleClusterNodesChanged(): Received the list of partitions from router " + _partitions.toString());
  }

  @Override
  public void handleClusterShutdown()
  {
    logger.info("handleClusterShutdown() called");
  }

  public SenseiSystemInfo getSystemInfo() throws SenseiException
  {
    if (_partitions == null)
      throw new SenseiException("getSystemInfo called before cluster is connected!");
    try
    {
      return doGetSystemInfo(_networkClient, _partitions);
    } catch (Exception e)
    {
      throw new SenseiException(e.getMessage(), e);
    }
  }

  private SenseiSystemInfo doGetSystemInfo(PartitionedNetworkClient<Integer> networkClient, IntSet partitions) throws Exception
  {
    if (partitions != null && (partitions.size()) > 0)
    {
      SenseiSysRequestBPO.SysRequest msg = SenseiSysRequestBPOConverter.convert(new SenseiRequest());
      SenseiSystemInfo res = networkClient.sendMessage(partitions, msg, _sysScatterGatherHandler);
      return res;
    } else
    {
      logger.warn("no server exist to handle request.");
      return new SenseiSystemInfo();
    }
  }

}
