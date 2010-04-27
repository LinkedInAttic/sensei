package com.sensei.search.nodes;

import com.linkedin.norbert.cluster.Node;
import com.sensei.search.req.SenseiRequest;

public interface SenseiRequestScatterRewriter {
	SenseiRequest rewrite(SenseiRequest origReq,Node node,Integer[] partitions);
}
