package com.bigdata.bdp.exception;

public class BDPException extends Exception{
	private static final long serialVersionUID = 1L;
	
	public BDPException(String message){
		super(message);
	}
	
	public BDPException(String message, Throwable throwable){
		super(message,throwable);
	}
}
