package com.senseidb.search.node;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.protobuf.Message;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiSystemInfo;

public class SenseiSysScatterGatherHandler extends AbstractSenseiScatterGatherHandler<SenseiRequest, SenseiSystemInfo>
{

  private final static Logger logger = Logger.getLogger(SenseiSysScatterGatherHandler.class);

  private final Comparator<String> _versionComparator;

  public SenseiSysScatterGatherHandler(SenseiRequest request, Comparator<String> versionComparator)
  {
      super(request);
      _versionComparator = versionComparator;
  }

  @Override
  public SenseiRequest customizeRequest(SenseiRequest senseiRequest, Node node, Set<Integer> partitions) {
    senseiRequest.setPartitions(partitions);
    return senseiRequest;
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
      if (result.getVersion() == null || _versionComparator.compare(result.getVersion(), res.getVersion()) < 0)
        result.setVersion(res.getVersion());
      if (res.getFacetInfos() != null)
        result.setFacetInfos(res.getFacetInfos());
      if (res.getClusterInfo() != null) {
        if (result.getClusterInfo() != null)
          result.getClusterInfo().addAll(res.getClusterInfo());
        else
          result.setClusterInfo(res.getClusterInfo());
      }
    }

    return result;
  }
}

