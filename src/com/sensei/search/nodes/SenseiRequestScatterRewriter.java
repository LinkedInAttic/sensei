package com.sensei.search.nodes;

import java.util.Set;

import com.linkedin.norbert.cluster.javaapi.Node;
import com.sensei.search.req.SenseiRequest;

public interface SenseiRequestScatterRewriter {
	SenseiRequest rewrite(SenseiRequest origReq,Node node, Set<Integer> partitions);
}
