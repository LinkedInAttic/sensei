package com.sensei.test.mocks.norbert;

import java.net.InetSocketAddress;
import java.util.HashMap;

import com.google.protobuf.Message;
import com.linkedin.norbert.cluster.Node;
import com.linkedin.norbert.network.NetworkingException;
import com.linkedin.norbert.network.javaapi.MessageHandler;
import com.linkedin.norbert.network.javaapi.NetworkServer;

public class MockNetworkServer implements NetworkServer {
    private InetSocketAddress _addr;
    private HashMap<String,MessageHandler> _msgHandlerMap;
    
	public MockNetworkServer(InetSocketAddress addr,MessageHandler[] msgHandlers) {
		_addr = addr;
		_msgHandlerMap = new HashMap<String,MessageHandler>();
		if (msgHandlers!=null){
			for (MessageHandler handler : msgHandlers){
				Message[] msgs = handler.getMessages();
				for (Message msg : msgs){
					String key = msg.getClass().getName();
					_msgHandlerMap.put(key, handler);
				}
			}
		}
	}
	
	public InetSocketAddress getAddress(){
		return _addr;
	}
	
	public MessageHandler getMessageHandler(Message msg){
		if (msg!=null){
			String key = msg.getClass().getName();
			return _msgHandlerMap.get(key);
		}
		return null;
	}

	public void bind() throws NetworkingException {
		MockServerHome.bind(this);
	}

	public void bind(boolean markAvailable) throws NetworkingException {
		
	}

	public Node getCurrentNode() {
		return null;
	}

	public void markAvailable() {

	}

	public void shutdown() {
		MockServerHome.unbind(this);
	}

}
