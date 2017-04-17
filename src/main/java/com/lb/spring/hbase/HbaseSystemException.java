package com.lb.spring.hbase;

public class HbaseSystemException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public HbaseSystemException() {
		super();
	}

	public HbaseSystemException(String message) {
		super(message);
	}
	
	public HbaseSystemException(String message, Throwable cause) {
		super(message, cause);
	}

	public HbaseSystemException(Throwable cause) {
		super(cause);
	}
	

}
