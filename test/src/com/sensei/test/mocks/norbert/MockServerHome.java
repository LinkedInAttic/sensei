package com.sensei.test.mocks.norbert;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.Message;
import com.linkedin.norbert.network.NetworkingException;
import com.linkedin.norbert.network.javaapi.MessageHandler;
import com.linkedin.norbert.network.javaapi.NetworkServer;

public class MockServerHome {
	private static Map<InetSocketAddress,MockNetworkServer> serverMap;
	
	static{
		serverMap = new HashMap<InetSocketAddress,MockNetworkServer>();
	}
	
	public static NetworkServer connect(InetSocketAddress addr){
		synchronized(serverMap){
		  return serverMap.get(addr);
		}
	}
	
	public static void bind(MockNetworkServer server) throws NetworkingException{
		InetSocketAddress addr = server.getAddress();
		synchronized(serverMap){
			NetworkServer boundServer = serverMap.get(addr);
			if (boundServer==null){
				serverMap.put(addr,server);
			}
			else{
				throw new NetworkingException(addr+" already bound to another server");
			}
		}
	}
	
	public static void unbind(MockNetworkServer server){
		InetSocketAddress addr = server.getAddress();
		synchronized(serverMap){
			serverMap.remove(addr);
		}
	}
	
	public static MessageHandler getMessageHandler(InetSocketAddress addr,Message msg){
		MockNetworkServer svr = null;
		synchronized(serverMap){
			svr = serverMap.get(addr);
		}
		if (svr!=null){
			return svr.getMessageHandler(msg);
		}
		return null;
	}
}
