/*******************************************************************************
* Copyright StreamAnalytix and Impetus Technologies.
* All rights reserved
*******************************************************************************/

package com.streamanalytix.extensions.exceptions;

/**
 * 
 * Exception class for throwing when any service is not available.
 *
 */
public class ServiceNotAvailableException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	/**
	 * @param msg This parameter is an exception message
	 */
	public ServiceNotAvailableException(String msg) {
		super(msg);
	}

	/**
	 * @param msg This parameter is an exception message
	 * @param th This parameter is an Throwable object
	 */
	public ServiceNotAvailableException(String msg, Throwable th) {
		super(msg, th);
	}

	/**
	 * @param th This parameter is Throwable object
	 */
	public ServiceNotAvailableException(Throwable th) {
		super(th);
	}
}
