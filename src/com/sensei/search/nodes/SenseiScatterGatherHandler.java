package com.sensei.search.nodes;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.protobuf.Message;
import com.google.protobuf.TextFormat.ParseException;
import com.linkedin.norbert.cluster.Node;
import com.linkedin.norbert.network.javaapi.Response;
import com.linkedin.norbert.network.javaapi.ResponseIterator;
import com.linkedin.norbert.network.javaapi.ScatterGatherHandler;
import com.sensei.search.client.ResultMerger;
import com.sensei.search.req.SenseiRequest;
import com.sensei.search.req.SenseiResult;
import com.sensei.search.req.protobuf.SenseiRequestBPO;
import com.sensei.search.req.protobuf.SenseiRequestBPOConverter;
import com.sensei.search.req.protobuf.SenseiResultBPO;

public class SenseiScatterGatherHandler implements ScatterGatherHandler<SenseiResult> {

	private final static Logger logger = Logger.getLogger(SenseiScatterGatherHandler.class);
	private final static long TIMEOUT_MILLIS = 8000;
	
	private final SenseiRequestScatterRewriter _reqRewriter;
	
	public SenseiScatterGatherHandler(SenseiRequestScatterRewriter reqRewriter){
		_reqRewriter = reqRewriter;
	}
	
	public Message customizeMessage(Message msg, Node node, int[] partitions) {
		SenseiRequestBPO.Request req = (SenseiRequestBPO.Request)msg;
		try{
		  SenseiRequest senseiReq = SenseiRequestBPOConverter.convert(req);
		  
		  int oldOffset = senseiReq.getOffset();
		  int oldCount = senseiReq.getCount();
		  
		  if (_reqRewriter!=null){
			  senseiReq = _reqRewriter.rewrite(senseiReq, node, partitions);
		  }
		  
		  senseiReq.setOffset(0);
		  senseiReq.setCount(oldOffset+oldCount);
		  senseiReq.setPartitions(partitions);
		  //logger.info("scattering to partitions: "+Arrays.toString(partitions));
		  return SenseiRequestBPOConverter.convert(senseiReq);
		}
		catch(ParseException pe){
		  throw new RuntimeException(pe.getMessage(),pe);
		} 
	}

	public SenseiResult gatherResponses(Message message, ResponseIterator iter) {
		SenseiRequestBPO.Request req = (SenseiRequestBPO.Request)message;
		try{
		  SenseiRequest senseiReq = SenseiRequestBPOConverter.convert(req);
		  int oldOffset = senseiReq.getOffset();
		  int oldCount = senseiReq.getCount();
		  
		  List<SenseiResult> boboBrowseList = new ArrayList<SenseiResult>();
		  while(iter.hasNext()){
			Response resp = iter.next(TIMEOUT_MILLIS,TimeUnit.MILLISECONDS);
			if (resp==null){
	    	  logger.error("Request Timed Out");
	    	}
	    	else {
			  if (resp.isSuccess()){
	    	    Message boboMsg = resp.getMessage();
	    	    SenseiResult res = SenseiRequestBPOConverter.convert((SenseiResultBPO.Result)boboMsg);
	    	    boboBrowseList.add(res);
	    	  }
	    	  else{
	    	    logger.error("Request Failed: ",resp.getCause());
	    	  }
	    	}
		  }
		      
		  senseiReq.setOffset(oldOffset);
		  senseiReq.setCount(oldCount);
		  SenseiResult res = ResultMerger.merge(senseiReq, boboBrowseList);
		  return res;
		}
		catch(ParseException pe){
		  throw new RuntimeException(pe.getMessage(),pe);
		}
	}

}
