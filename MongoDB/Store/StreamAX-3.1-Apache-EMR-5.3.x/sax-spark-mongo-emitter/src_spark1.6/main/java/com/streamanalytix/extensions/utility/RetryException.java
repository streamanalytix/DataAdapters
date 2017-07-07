/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.utility;

/** The Class RetryException. */
public class RetryException extends Exception {

    /** Instantiates a new retry exception.
     *
     * @param msg
     *            "exception message for throwing exception" */
    public RetryException(String msg) {
        super(msg);
    }

}
