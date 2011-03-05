package com.sensei.search.nodes;

import org.apache.log4j.Logger;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.network.NetworkServer;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiResultBPO;

public class SenseiNode extends AbstractSenseiNode
{
  private static Logger logger = Logger.getLogger(SenseiNode.class);

  public SenseiNode(NetworkServer server, ClusterClient client, int id, int port, SenseiSearchContext context, int[] partitions)
  {
    super(server, client, id, port, context, partitions);
  }

  @Override
  public void initMessageHandlers()
  {
    SenseiNodeMessageHandler msgHandler = new SenseiNodeMessageHandler(_context);
    _server.registerHandler(SenseiRequestBPO.Request.getDefaultInstance(), SenseiResultBPO.Result.getDefaultInstance(), msgHandler);
  }
}
