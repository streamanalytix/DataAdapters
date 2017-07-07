/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/

package com.streamanalytix.extensions.exceptions;

/** Exception class for throwing when any service is not available. */
public class ServiceNotAvailableException extends RuntimeException {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** Instantiates a new service not available exception.
     *
     * @param msg
     *            the msg */
    public ServiceNotAvailableException(String msg) {
        super(msg);
    }

    /** Instantiates a new service not available exception.
     *
     * @param msg
     *            the msg
     * @param th
     *            the th */
    public ServiceNotAvailableException(String msg, Throwable th) {
        super(msg, th);
    }

    /** Instantiates a new service not available exception.
     *
     * @param th
     *            the th */
    public ServiceNotAvailableException(Throwable th) {
        super(th);
    }
}
