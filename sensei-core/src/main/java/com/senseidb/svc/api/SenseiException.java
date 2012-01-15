package com.senseidb.svc.api;

public class SenseiException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public SenseiException() {
	}

	public SenseiException(String message) {
		super(message);
	}

	public SenseiException(Throwable cause) {
		super(cause);
	}

	public SenseiException(String message, Throwable cause) {
		super(message, cause);
	}

}
