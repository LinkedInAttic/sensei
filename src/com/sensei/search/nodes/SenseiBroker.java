package com.sensei.search.nodes;

import it.unimi.dsi.fastutil.ints.IntSet;

import org.apache.log4j.Logger;

import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.cluster.Node;
import com.linkedin.norbert.cluster.javaapi.Cluster;
import com.linkedin.norbert.cluster.javaapi.ClusterListenerAdapter;
import com.linkedin.norbert.cluster.javaapi.Router;
import com.linkedin.norbert.network.javaapi.NetworkClient;
import com.sensei.search.cluster.routing.UniformRouter;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiRequestBPOConverter;
import com.sensei.search.svc.api.SenseiException;

public class SenseiBroker extends ClusterListenerAdapter  {
	private final static Logger logger = Logger.getLogger(SenseiBroker.class);
	private final Cluster _cluster;
	
	private final NetworkClient _networkClient;
	
	private volatile UniformRouter _router = null;
	
	private final SenseiRequestScatterRewriter _reqRewriter;
	private final SenseiScatterGatherHandler _scatterGatherHandler;
	
	private SenseiResult doBrowse(NetworkClient networkClient,SenseiRequest req,IntSet partitions) throws Exception{
		int size = 0;
		if (partitions!=null && (size=partitions.size())>0){
		  if (req.getQuery() == null){
			  req.setQuery("");
		  }
		  
		  SenseiRequestBPO.Request msg = SenseiRequestBPOConverter.convert(req);
		  int[] partToSend = req.getPartitions();
		  if (partToSend==null){
			  partToSend = partitions.toIntArray();
		  }
		  SenseiResult res;
		  if (partToSend.length>0){
		    res = networkClient.sendMessage(partitions.toIntArray(), msg, _scatterGatherHandler);
		  }
		  else{
			res = new SenseiResult();  
		  }
	      return res;
		}
		else{
			logger.warn("no server exist to handle request.");
			return new SenseiResult();
		}
	}
	
	public SenseiBroker(Cluster cluster,NetworkClient networkClient,SenseiRequestScatterRewriter reqRewriter) throws NorbertException{
		_cluster = cluster;
		_cluster.addListener(this);
		_router = (UniformRouter)_cluster.getRouter();
		
		_networkClient = networkClient;
		_reqRewriter = reqRewriter;
		_scatterGatherHandler = new SenseiScatterGatherHandler(_reqRewriter);
	}
	
	public SenseiResult browse(SenseiRequest req) throws SenseiException{
		UniformRouter router = _router;
		IntSet parts = router.getPartitions();
		try {
			return doBrowse(_networkClient,req,parts);
		} catch (Exception e) {
			throw new SenseiException(e.getMessage(),e);
		}
	}
	
	@Override
	public void handleClusterConnected(Node[] nodes, Router router) {
		_router = (UniformRouter)router;
	}


	@Override
	public void handleClusterNodesChanged(Node[] nodes, Router router) {
		_router = (UniformRouter)router;
	}
	
	public void shutdown(){
	}
}
