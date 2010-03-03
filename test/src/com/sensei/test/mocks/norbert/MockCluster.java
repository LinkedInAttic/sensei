package com.sensei.test.mocks.norbert;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.linkedin.norbert.cluster.ClusterDisconnectedException;
import com.linkedin.norbert.cluster.ClusterShutdownException;
import com.linkedin.norbert.cluster.InvalidClusterException;
import com.linkedin.norbert.cluster.InvalidNodeException;
import com.linkedin.norbert.cluster.Node;
import com.linkedin.norbert.cluster.javaapi.Cluster;
import com.linkedin.norbert.cluster.javaapi.ClusterListener;
import com.linkedin.norbert.cluster.javaapi.Router;
import com.linkedin.norbert.cluster.javaapi.RouterFactory;

public class MockCluster implements Cluster {
    private final List<ClusterListener> _lsnrList;
    private boolean _isShutdown;
    private Int2ObjectOpenHashMap<Node> _nodeMap;
    private RouterFactory _routerFactory;
    
	public MockCluster(Int2ObjectOpenHashMap<Node> nodeMap){
		_lsnrList = new LinkedList<ClusterListener>();
		_nodeMap = nodeMap;
		_isShutdown = false;
		_routerFactory = null;
	}
	
	public void setRouterFactory(RouterFactory routerFactory){
		_routerFactory = routerFactory;
	}
	
	public void addListener(ClusterListener lsnr)
			throws ClusterShutdownException {
		_lsnrList.add(lsnr);
	}

	public Node addNode(int nodeId, InetSocketAddress addr, int[] partitions)
			throws ClusterDisconnectedException, InvalidNodeException {
		Node node = new Node(nodeId,addr,partitions,false);
		synchronized(_nodeMap){
		  if (_nodeMap.containsKey(nodeId)) throw new InvalidNodeException("node with id: "+nodeId+" already exists.");
		  _nodeMap.put(nodeId, node);
		}
		for (ClusterListener lsnr : _lsnrList){
			try {
				lsnr.handleClusterNodesChanged(getNodes(), getRouter());
			} catch (ClusterShutdownException e) {
				e.printStackTrace();
			}
		}
		return node;
	}

	public void awaitConnection() throws ClusterShutdownException,
			InterruptedException {
		awaitConnectionUninterruptibly();
	}

	public boolean awaitConnection(long time, TimeUnit timeUnit)
			throws ClusterShutdownException {
		awaitConnectionUninterruptibly();
		return true;
	}

	public void awaitConnectionUninterruptibly()
			throws ClusterShutdownException {
		for (ClusterListener lsnr : _lsnrList){
			try {
				lsnr.handleClusterConnected(getNodes(), getRouter());
			} catch (ClusterShutdownException e) {
				e.printStackTrace();
			}
		}
	}

	public Node getNodeWithAddress(InetSocketAddress address)
			throws ClusterDisconnectedException {
		synchronized(_nodeMap){
			Collection<Node> nodes = _nodeMap.values();
			for (Node node : nodes){
				InetSocketAddress addr = node.getAddress();
				if (addr.equals(address)){
					return node;
				}
			}
			return null;
		}
	}

	public Node getNodeWithId(int id) throws ClusterDisconnectedException {
		synchronized(_nodeMap){
			return _nodeMap.get(id);
		}
	}

	public Node[] getNodes() throws ClusterShutdownException {
		synchronized(_nodeMap){
			Collection<Node> valueSet = _nodeMap.values();
			return valueSet.toArray(new Node[valueSet.size()]);
		}
	}

	public Router getRouter() throws ClusterShutdownException {
		try {
			return _routerFactory == null ? null : _routerFactory.newRouter(getNodes());
		} catch (InvalidClusterException e) {
			e.printStackTrace();
			return null;
		}
	}

	public boolean isConnected() {
		return true;
	}

	public boolean isShutdown() {
		return _isShutdown;
	}

	public void markNodeAvailable(int nodeId) throws ClusterDisconnectedException {
		synchronized (_nodeMap) {
			Node node = getNodeWithId(nodeId);
			if (node!=null){
				Node newNode = new Node(nodeId,node.getAddress(),node.getPartitions(),true);
				_nodeMap.put(nodeId,newNode);
			}
		}
		for (ClusterListener lsnr : _lsnrList){
			try {
				lsnr.handleClusterNodesChanged(getNodes(), getRouter());
			} catch (ClusterShutdownException e) {
				e.printStackTrace();
			}
		}
	}

	public void removeListener(ClusterListener lsnr)
			throws ClusterShutdownException {
		_lsnrList.remove(lsnr);
	}

	public void removeNode(int nodeId) throws ClusterDisconnectedException,
			InvalidNodeException {
		synchronized(_nodeMap){
		  _nodeMap.remove(nodeId);
		}
		for (ClusterListener lsnr : _lsnrList){
			try {
				lsnr.handleClusterNodesChanged(getNodes(), getRouter());
			} catch (ClusterShutdownException e) {
				e.printStackTrace();
			}
		}
	}

	public void shutdown() {
		_isShutdown = true;
		for (ClusterListener lsnr : _lsnrList){
			lsnr.handleClusterShutdown();
		}
	}

}
