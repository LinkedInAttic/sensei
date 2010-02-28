package com.sensei.test.mocks.norbert;

import java.util.concurrent.TimeUnit;

import com.google.protobuf.Message;
import com.linkedin.norbert.network.javaapi.Response;
import com.linkedin.norbert.network.javaapi.ResponseIterator;

public class MockResponseIterator implements ResponseIterator {
	private final Response[] _responses;
	private int _current;
	
	public MockResponseIterator(Message[] msgs){
		_responses = new Response[msgs.length];
		for(int i=0;i<msgs.length;++i){
			_responses[i] = new MockResponse(msgs[i]);
		}
		_current = 0;
	}
	
	@Override
	public boolean hasNext() {
		return _current < _responses.length;
	}

	@Override
	public Response next() {
		return _responses[_current++];
	}

	@Override
	public Response next(long timeOut, TimeUnit arg1) {
		return next();
	}

	public static class MockResponse implements Response{
		private final Message _msg;
		private final Throwable _throwable;
		
		private MockResponse(Message msg,Throwable throwable){
			_msg = msg;
			_throwable = throwable;
		}
		
		public MockResponse(Message msg){
			this(msg,null);
		}
		
		public MockResponse(Throwable throwable){
			this(null,throwable);
		}
		
		@Override
		public Throwable getCause() {
			return _throwable;
		}

		@Override
		public Message getMessage() {
			return _msg;
		}

		@Override
		public boolean isSuccess() {
			return _throwable == null;
		}
		
	}
}
