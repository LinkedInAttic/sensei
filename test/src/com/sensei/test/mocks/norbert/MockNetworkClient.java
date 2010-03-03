package com.sensei.test.mocks.norbert;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

import com.google.protobuf.Message;
import com.linkedin.norbert.cluster.ClusterShutdownException;
import com.linkedin.norbert.cluster.InvalidNodeException;
import com.linkedin.norbert.cluster.Node;
import com.linkedin.norbert.cluster.javaapi.Cluster;
import com.linkedin.norbert.cluster.javaapi.Router;
import com.linkedin.norbert.network.javaapi.MessageHandler;
import com.linkedin.norbert.network.javaapi.NetworkClient;
import com.linkedin.norbert.network.javaapi.ResponseIterator;
import com.linkedin.norbert.network.javaapi.ScatterGatherHandler;

public class MockNetworkClient implements NetworkClient {

	private final Cluster _cluster;
	public MockNetworkClient(Cluster cluster){
		_cluster = cluster;
	}
	
	
	@Override
	public void close() throws ClusterShutdownException {

	}

	@Override
	public boolean isConnected() {
		return true;
	}

	private HashMap<Node,IntList> buildNodeToPartList(int[] partitions) throws ClusterShutdownException{
		Router router = _cluster.getRouter();
		HashMap<Node,IntList> nodeToPartList = new HashMap<Node,IntList>();
		if (router!=null){
			for (int partition : partitions){
				Node node = router.calculateRoute(partition);
				IntList partList = nodeToPartList.get(node);
				if (partList==null){
					partList = new IntArrayList();
					nodeToPartList.put(node,partList);
				}
				partList.add(partition);
			}
		}
		return nodeToPartList;
	}
	
	@Override
	public ResponseIterator sendMessage(int[] partitions, Message req)
			throws ClusterShutdownException {
		HashMap<Node,IntList> nodeToPartList = buildNodeToPartList(partitions);
		Set<Entry<Node,IntList>> entrySet = nodeToPartList.entrySet();
		ArrayList<Message> responseList = new ArrayList<Message>(partitions.length);
		for (Entry<Node,IntList> entry : entrySet){
			Node node = entry.getKey();
			IntList partList = entry.getValue();
			for (int p : partList){
				Message resp = doSendMessage(node, req);
				responseList.add(resp);
			}
		}
		return new MockResponseIterator(responseList.toArray(new Message[responseList.size()]));
	}

	@Override
	public <A> A sendMessage(int[] partitions, Message req,
			ScatterGatherHandler<A> sgHandler) throws ClusterShutdownException,
			Exception {
		HashMap<Node,IntList> nodeToPartList = buildNodeToPartList(partitions);
		Set<Entry<Node,IntList>> entrySet = nodeToPartList.entrySet();
		ArrayList<Message> responseList = new ArrayList<Message>(partitions.length);
		for (Entry<Node,IntList> entry : entrySet){
			Node node = entry.getKey();
			IntList partList = entry.getValue();

			Message msg = sgHandler.customizeMessage(req, node, partList.toIntArray());
			Message resp = doSendMessage(node, msg);
			responseList.add(resp);
		}
		return sgHandler.gatherResponses(req, new MockResponseIterator(responseList.toArray(new Message[responseList.size()])));
	}
	
	public Message doSendMessage(Node node, Message req){
		if (node!=null){
			InetSocketAddress addr = node.getAddress();
			MessageHandler handler = MockServerHome.getMessageHandler(addr, req);
			try {
				if (handler!=null){
				  return handler.handleMessage(req);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	@Override
	public ResponseIterator sendMessageToNode(Node node, Message req)
			throws ClusterShutdownException, InvalidNodeException {

		Message resp = doSendMessage(node, req);
		if (resp!=null){
			return new MockResponseIterator(new Message[]{resp});
		}
		return null;
	}

}
