package com.senseidb.search.node.impl;

import java.util.Set;

import com.linkedin.norbert.javacompat.cluster.Node;
import com.senseidb.search.node.SenseiRequestScatterRewriter;
import com.senseidb.search.req.SenseiRequest;

public class NoopRequestScatterRewriter implements SenseiRequestScatterRewriter
{

  @Override
  public SenseiRequest rewrite(SenseiRequest origReq, Node node, Set<Integer> partitions)
  {
    return origReq;
  }
}
