package com.sensei.search.nodes.impl;

import java.util.Set;

import com.linkedin.norbert.cluster.javaapi.Node;
import com.sensei.search.nodes.SenseiRequestScatterRewriter;
import com.sensei.search.req.SenseiRequest;

public class NoopRequestScatterRewriter implements SenseiRequestScatterRewriter
{

  @Override
  public SenseiRequest rewrite(SenseiRequest origReq, Node node, Set<Integer> partitions)
  {
    return origReq;
  }
}
