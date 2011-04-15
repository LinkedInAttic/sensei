package com.sensei.search.nodes;

import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.protobuf.Message;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiSystemInfo;
import com.sensei.search.req.protobuf.SenseiSysRequestBPO;
import com.sensei.search.req.protobuf.SenseiSysRequestBPOConverter;
import com.sensei.search.req.protobuf.SenseiSysResultBPO;

public class SenseiSysScatterGatherHandler extends AbstractSenseiScatterGatherHandler<SenseiRequest, SenseiSystemInfo, SenseiSysRequestBPO.SysRequest, SenseiSysResultBPO.SysResult>
{

  private final static Logger logger = Logger.getLogger(SenseiSysScatterGatherHandler.class);

  public Message customizeMessage(Message msg, Node node, Set<Integer> partitions) throws Exception
  {
    SenseiSysRequestBPO.SysRequest req = (SenseiSysRequestBPO.SysRequest) msg;
    SenseiRequest senseiReq = SenseiSysRequestBPOConverter.convert(req);

    senseiReq.setPartitions(partitions);

    return SenseiSysRequestBPOConverter.convert(senseiReq);
  }

  @Override
  public SenseiSystemInfo mergeResults(SenseiRequest request, List<SenseiSystemInfo> resultList)
  {
    SenseiSystemInfo result = new SenseiSystemInfo();
    if (resultList == null)
      return result;

    for (SenseiSystemInfo res : resultList)
    {
      result.setNumDocs(result.getNumDocs()+res.getNumDocs());
      if (result.getLastModified() < res.getLastModified())
        result.setLastModified(res.getLastModified());
      try
      {
        // TODO: we need the new zoie version comparator patten.
        if (Long.valueOf(result.getVersion()) < Long.valueOf(res.getVersion()))
          result.setVersion(res.getVersion());
      }
      catch (Exception e)
      {
      }
      if (res.getFacetInfos() != null)
        result.setFacetInfos(res.getFacetInfos());
      if (res.getClusterInfo() != null) {
        if (result.getClusterInfo() != null)
          result.getClusterInfo().putAll(res.getClusterInfo());
        else
          result.setClusterInfo(res.getClusterInfo());
      }
    }

    return result;
  }

  @Override
  public SenseiRequest messageToRequest(SenseiSysRequestBPO.SysRequest msg)
  {
    return SenseiSysRequestBPOConverter.convert(msg);
  }

  @Override
  public SenseiSystemInfo messageToResult(SenseiSysResultBPO.SysResult message)
  {
    return SenseiSysRequestBPOConverter.convert(message);
  }

  @Override
  public SenseiSysRequestBPO.SysRequest requestToMessage(SenseiRequest request)
  {
    return SenseiSysRequestBPOConverter.convert(request);
  }

  @Override
  public SenseiSysResultBPO.SysResult resultToMessage(SenseiSystemInfo result)
  {
    return SenseiSysRequestBPOConverter.convert(result);
  }
}

